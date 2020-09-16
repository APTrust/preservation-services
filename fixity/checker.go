package fixity

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/registry"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/network"
	"github.com/APTrust/preservation-services/restoration"
	"github.com/minio/minio-go/v6"
	uuid "github.com/satori/go.uuid"
)

type Checker struct {
	// Context is the context, which includes config settings and
	// clients to access S3 and Pharos.
	Context *common.Context

	// Identifier is the identifier of the GenericFile whose fixity
	// we're checking.
	Identifier string
}

// NewChecker creates a new fixity.Checker.
func NewChecker(context *common.Context, identifier string) *Checker {
	return &Checker{
		Context:    context,
		Identifier: identifier,
	}
}

func (c *Checker) Run() (count int, errors []*service.ProcessingError) {
	gf, err := c.GetGenericFile()
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	if c.IsGlacierOnlyFile(gf) {
		c.Context.Logger.Info("Skipping file %s because it's Glacier-only", gf.Identifier)
		return 0, errors
	}
	checksum, err := c.GetLatestSha256()
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	actualFixity, url, err := c.CalculateFixity(gf)
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	fixityMatched, err := c.RecordFixityEvent(gf, url, checksum.Digest, actualFixity)
	if err != nil {
		errors = append(errors, c.Error(err, true))
		return 0, errors
	}
	count = 1
	if !fixityMatched {
		err = fmt.Errorf("Fixity mismatch for %s in %s. Expected %s, got %s.", gf.Identifier, url, checksum.Digest, actualFixity)
		errors = append(errors, c.Error(err, true))
	}
	return count, errors
}

func (c *Checker) GetGenericFile() (*registry.GenericFile, error) {
	resp := c.Context.PharosClient.GenericFileGet(c.Identifier)
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.GenericFile(), nil
}

func (c *Checker) IsGlacierOnlyFile(gf *registry.GenericFile) bool {
	return strings.HasPrefix(gf.StorageOption, "Glacier")
}

func (c *Checker) GetLatestSha256() (checksum *registry.Checksum, err error) {
	params := url.Values{}
	params.Set("generic_file_identifier", c.Identifier)
	params.Set("algorithm", constants.AlgSha256)
	resp := c.Context.PharosClient.ChecksumList(params)
	if resp.Error != nil {
		return nil, resp.Error
	}
	// I don't trust Pharos to sort correctly.
	for _, cs := range resp.Checksums() {
		if checksum == nil || cs.DateTime.After(checksum.DateTime) {
			checksum = cs
		}
	}
	if checksum == nil {
		err = fmt.Errorf("Pharos returned no sha256 checksum for file %s", c.Identifier)
	}
	return checksum, err
}

func (c *Checker) CalculateFixity(gf *registry.GenericFile) (fixity, url string, err error) {
	// TODO: Stream S3 download through sha256 hash.
	preservationBucket, storageRecord, err := restoration.BestRestorationSource(c.Context, gf)
	if err != nil {
		return "", "", err
	}
	client := c.Context.S3Clients[preservationBucket.Provider]
	if client == nil {
		return "", "", fmt.Errorf("Cannot find S3 client for provider %s", preservationBucket.Provider)
	}
	obj, err := client.GetObject(
		preservationBucket.Bucket,
		gf.UUID(),
		minio.GetObjectOptions{},
	)
	if err != nil {
		err = fmt.Errorf("Error getting %s from S3 (%s): %v", gf.Identifier, storageRecord.URL, err)
		return "", storageRecord.URL, err
	}
	defer obj.Close()

	sha256Hash := sha256.New()
	_, err = io.Copy(sha256Hash, obj)
	if err != nil {
		err = fmt.Errorf("Error streaming S3 file %s/%s through hash function: %v", preservationBucket.Bucket, gf.UUID(), err)
		return "", storageRecord.URL, err
	}
	fixity = fmt.Sprintf("%x", sha256Hash.Sum(nil))
	return fixity, storageRecord.URL, err
}

func (c *Checker) RecordFixityEvent(gf *registry.GenericFile, url, expectedFixity, actualFixity string) (fixityMatched bool, err error) {
	fixityMatched = expectedFixity == actualFixity
	event := c.GetFixityEvent(gf, url, expectedFixity, actualFixity)

	// Still need to work out 502s between nginx and Pharos when Pharos is busy
	var resp *network.PharosResponse
	for i := 0; i < 3; i++ {
		resp = c.Context.PharosClient.PremisEventSave(event)
		if resp.Response.StatusCode != http.StatusBadGateway {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return fixityMatched, resp.Error
}

func (c *Checker) GetFixityEvent(gf *registry.GenericFile, url, expectedFixity, actualFixity string) *registry.PremisEvent {
	eventId := uuid.NewV4()
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
		Agent:                        agent,
		DateTime:                     time.Now().UTC(),
		Detail:                       "Fixity check against registered hash",
		EventType:                    constants.EventFixityCheck,
		GenericFileID:                gf.ID,
		GenericFileIdentifier:        gf.Identifier,
		Identifier:                   eventId.String(),
		InstitutionID:                gf.InstitutionID,
		IntellectualObjectID:         gf.IntellectualObjectID,
		IntellectualObjectIdentifier: gf.IntellectualObjectIdentifier,
		Object:                       object,
		Outcome:                      outcome,
		OutcomeDetail:                fmt.Sprintf("%s:%s", constants.AlgSha256, actualFixity),
		OutcomeInformation:           outcomeInformation,
	}
}

// IngestObjectGet is a dummy method that allows this object to conform to the
// ingest.Runnable interface.
func (c *Checker) IngestObjectGet() *service.IngestObject {
	return nil
}

// IngestObjectSave is a dummy method that allows this object to conform to the
// ingest.Runnable interface.
func (c *Checker) IngestObjectSave() error {
	return nil
}

func (c *Checker) Error(err error, isFatal bool) *service.ProcessingError {
	return service.NewProcessingError(
		0,
		c.Identifier,
		err.Error(),
		isFatal,
	)
}
