package transfer

import (
	ctx "context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
)

// Based on ingest_preservation_verifier and fixity_checker

// ObjectTransferValidator verifies that all files were copied correctly
// to preservation storage.
type ObjectTransferValidator struct {
	Base
}

// NewObjectTransferValidator returns a new ObjectTransferValidator.
func NewObjectTransferValidator(context *common.Context, workItemID int64, transferObject *service.TransferObject) *ObjectTransferValidator {
	return &ObjectTransferValidator{
		Base: Base{
			Context:        context,
			TransferObject: transferObject,
			WorkItemID:     workItemID,
		},
	}
}

// Run verifies that all files were copied to preservation storage.
// Note that this relies on the StorageRecords attached to each IngestFile.
// It does not attempt to ensure that the StorageRecords themselves are
// valid for the StorageOption. The PerservationUploader is responsible for
// that. This just verifies that the PreservationUploader did what it said
// it did.
func (v *ObjectTransferValidator) Run() (int, []*service.ProcessingError) {
	verifyFn := v.getVerifyFunction()
	options := service.TransferFileApplyOptions{
		MaxErrors:   30,
		MaxRetries:  3,
		RetryMs:     1000,
		SaveChanges: true,
		WorkItemID:  v.WorkItemID,
	}
	return v.Context.RedisClient.TransferFilesApply(verifyFn, options)
}

func (v *ObjectTransferValidator) getVerifyFunction() func(*service.TransferFile) (errors []*service.ProcessingError) {
	return func(transferFile *service.TransferFile) (errors []*service.ProcessingError) {
		// Ensure that this is pulling from the NEW record in target buckets, NOT the old record!
		for _, record := range transferFile.StorageRecords {
			v.Context.Logger.Infof("Verifying %s (%s) in %s %s", transferFile.Identifier(), transferFile.UUID, record.Provider, record.Bucket)
			objInfo, err := v.Context.S3StatObject(
				record.Provider,
				record.Bucket,
				transferFile.UUID,
			)
			// Should check err type -> "no such key" should be fatal
			if err != nil {
				v.Context.Logger.Errorf("Error for %s (%s) in %s %s: %v", transferFile.Identifier(), transferFile.UUID, record.Provider, record.Bucket, err)
				errors = append(errors, v.Error(transferFile.Identifier(), err, false))
			} else {
				record.ETag = strings.Replace(objInfo.ETag, "\"", "", -1)
				record.Size = objInfo.Size
				if record.Size == transferFile.Size {
					v.Context.Logger.Infof("Verified %s (%s) is in %s %s with size %d and etag %s", transferFile.Identifier(), transferFile.UUID, record.Provider, record.Bucket, record.Size, record.ETag)
					record.VerifiedAt = time.Now().UTC()
				} else {
					err = fmt.Errorf("Preservation size %d does not match recorded file size %d", record.Size, transferFile.Size)
					v.Context.Logger.Errorf("Error for %s (%s) in %s %s: %v", transferFile.Identifier(), transferFile.UUID, record.Provider, record.Bucket, err)
					record.Error = err.Error()
					errors = append(errors, v.Error(transferFile.Identifier(), err, false))
				}
			}
		}

		// fixity check - if new files are in glacier or deep glacier?

		return errors
	}
}

type Checker struct {
	// Context is the context, which includes config settings and
	// clients to access S3 and Registry.
	Context *common.Context

	// GenericFileIdentifier is the identifier of the GenericFile whose fixity
	// we're checking.
	GenericFileIdentifier string

	// ID of the file we're checking.
	GenericFileID int64
}

// NewChecker creates a new fixity.Checker.
func NewChecker(context *common.Context, gfId int64) *Checker {
	return &Checker{
		Context:       context,
		GenericFileID: gfId,
	}
}

func (c *Checker) Run() (count int, errors []*service.ProcessingError) {
	gf, err := c.GetGenericFile()
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	c.Context.Logger.Infof("Got Registry record for %s", gf.Identifier)
	if c.IsGlacierOnlyFile(gf) {
		err = fmt.Errorf("Skipping file %s because it's Glacier-only", gf.Identifier)
		c.Context.Logger.Warningf("%v", err)
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	// When fixity checker has a backlog, some files may be queued twice.
	// We don't want to run the same fixity checks twice in one day, so
	// be sure this file actuall needs it. For more info, see
	// https://trello.com/c/vdyB325m
	// We skip this check on end-to-end tests because those tests ingest
	// files and then immediately schedule them for fixity check.
	expectedLastFixity := time.Now().UTC().AddDate(0, 0, (-1 * c.Context.Config.MaxDaysSinceFixityCheck))
	if gf.LastFixityCheck.After(expectedLastFixity) && !c.Context.Config.IsE2ETest() {
		c.Context.Logger.Infof("Skipping file %s (%d) because it had a fixity check on %s", gf.Identifier, gf.ID, gf.LastFixityCheck.Format(time.RFC3339))
		return 0, errors
	}

	checksum := gf.GetLatestChecksum(constants.AlgSha256)
	if checksum == nil {
		_err := fmt.Errorf("cannot find latest sha256 checksum for file %s (%d)", gf.Identifier, gf.ID)
		errors = append(errors, c.Error(_err, true))
		return 0, errors
	}

	actualFixity, url, err := c.CalculateFixity(gf)
	if err != nil {
		c.Context.Logger.Error(err)
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	c.Context.Logger.Infof("Preservation file %s (%d) has fixity %s", gf.Identifier, gf.ID, actualFixity)
	fixityMatched, err := c.RecordFixityEvent(gf, url, checksum.Digest, actualFixity)
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	count = 1
	if !fixityMatched {
		err = fmt.Errorf("Fixity mismatch for %s (%d) in %s. Expected %s, got %s.", gf.Identifier, gf.ID, url, checksum.Digest, actualFixity)
		errors = append(errors, c.Error(err, true))
	} else {
		c.Context.Logger.Infof("Fixity matched for %s (%d)", gf.Identifier, gf.ID)
	}
	return count, errors
}

func (c *Checker) GetGenericFile() (*registry.GenericFile, error) {
	resp := c.Context.RegistryClient.GenericFileByID(c.GenericFileID)
	if resp.Error != nil {
		return nil, resp.Error
	}
	gf := resp.GenericFile()
	c.GenericFileIdentifier = gf.Identifier
	return gf, nil
}

func (c *Checker) IsGlacierOnlyFile(gf *registry.GenericFile) bool {
	return strings.HasPrefix(gf.StorageOption, "Glacier")
}

func (c *Checker) CalculateFixity(gf *registry.GenericFile) (fixity, url string, err error) {
	// TODO: Stream S3 download through sha256 hash.
	preservationBucket, storageRecord, err := restoration.BestRestorationSource(c.Context, gf)
	if err != nil {
		c.Context.Logger.Errorf("Could not find restoration source for %s (%d): %v", gf.Identifier, gf.ID, err)
		return "", "", err
	}
	client := c.Context.S3Clients[preservationBucket.Bucket]
	if client == nil {
		err = fmt.Errorf("Cannot find S3 client for provider %s", preservationBucket.Provider)
		c.Context.Logger.Error(err.Error())
		return "", "", err
	}
	c.Context.Logger.Infof("Checking %s for file %s (%d) with UUID %s", preservationBucket.Bucket, gf.Identifier, gf.ID, gf.UUID)
	obj, err := client.GetObject(
		ctx.Background(),
		preservationBucket.Bucket,
		gf.UUID,
		minio.GetObjectOptions{},
	)
	if err != nil {
		err = fmt.Errorf("Error getting %s (%d) from S3 (%s): %v", gf.Identifier, gf.ID, storageRecord.URL, err)
		return "", storageRecord.URL, err
	}
	defer obj.Close()

	sha256Hash := sha256.New()
	_, err = io.Copy(sha256Hash, obj)
	if err != nil {
		err = fmt.Errorf("Error streaming S3 file %s/%s through hash function: %v", preservationBucket.Bucket, gf.UUID, err)
		return "", storageRecord.URL, err
	}
	fixity = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	return fixity, storageRecord.URL, err
}

func (c *Checker) RecordFixityEvent(gf *registry.GenericFile, url, expectedFixity, actualFixity string) (fixityMatched bool, err error) {
	fixityMatched = expectedFixity == actualFixity
	event := c.GetFixityEvent(gf, url, expectedFixity, actualFixity)

	// Still need to work out 502s between nginx and Pharos when Pharos is busy
	// TODO: Does this problem exist in Registry? Will have to test and see.
	var resp *network.RegistryResponse
	for i := 0; i < 3; i++ {
		resp = c.Context.RegistryClient.PremisEventSave(event)
		if resp.Response.StatusCode != http.StatusBadGateway {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return fixityMatched, resp.Error
}

func (c *Checker) GetFixityEvent(gf *registry.GenericFile, url, expectedFixity, actualFixity string) *registry.PremisEvent {
	eventId := uuid.New()
	object := "Go language crypto/sha256"
	agent := "http://golang.org/pkg/crypto/sha256/"
	outcomeInformation := fmt.Sprintf("Fixity matches at %s: %s", url, actualFixity)
	outcome := string(constants.StatusSuccess)
	if expectedFixity != actualFixity {
		outcome = string(constants.StatusFailed)
		outcomeInformation = fmt.Sprintf("Fixity did not match at %s. Expected %s, got %s", url, expectedFixity, actualFixity)
		c.Context.Logger.Errorf("GenericFile %s: %s", gf.Identifier, outcomeInformation)
	}
	return &registry.PremisEvent{
		Agent:                 agent,
		DateTime:              time.Now().UTC(),
		Detail:                "Fixity check against registered hash",
		EventType:             constants.EventFixityCheck,
		GenericFileID:         gf.ID,
		GenericFileIdentifier: gf.Identifier,
		Identifier:            eventId.String(),
		InstitutionID:         gf.InstitutionID,
		IntellectualObjectID:  gf.IntellectualObjectID,
		Object:                object,
		Outcome:               outcome,
		OutcomeDetail:         fmt.Sprintf("%s:%s", constants.AlgSha256, actualFixity),
		OutcomeInformation:    outcomeInformation,
	}
}

func (c *Checker) Error(err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		0,
		c.GenericFileIdentifier,
		err.Error(),
		isFatal,
	)
}
