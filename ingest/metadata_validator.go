package ingest

import (
	"fmt"
	"strings"

	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/constants"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
)

type MetadataValidator struct {
	Base
	Errors  []string
	Profile *bagit.Profile
}

func NewMetadataValidator(context *common.Context, profile *bagit.Profile, ingestObject *service.IngestObject, workItemID int) *MetadataValidator {
	return &MetadataValidator{
		Base: Base{
			Context:      context,
			IngestObject: ingestObject,
			WorkItemID:   workItemID,
		},
		Errors:  make([]string, 0),
		Profile: profile,
	}
}

func (v *MetadataValidator) Run() (fileCount int, errors []*service.ProcessingError) {
	// Validation errors are fatal. We can't ingest an invalid bag.
	if !v.IsValid() {
		for _, err := range v.Errors {
			errors = append(errors, v.Error(v.IngestObject.Identifier(), fmt.Errorf(err), true))
		}
	}
	return v.IngestObject.FileCount, errors
}

func (v *MetadataValidator) IsValid() bool {
	if !v.SerializationOk() {
		return false
	}
	if !v.BagItVersionOk() {
		return false
	}
	if !v.FetchTxtOk() {
		return false
	}
	if !v.ManifestsAllowedOk() {
		return false
	}
	if !v.ManifestsRequiredOk() {
		return false
	}
	if !v.TagFilesAllowedOk() {
		return false
	}
	if !v.TagManifestsAllowedOk() {
		return false
	}
	if !v.TagManifestsRequiredOk() {
		return false
	}
	if !v.HasAllRequiredTags() {
		return false
	}
	if !v.ExistingTagsOk() {
		return false
	}
	if !v.IngestFilesOk() {
		return false
	}
	return true
}

// Technically, we should check this. But the MetadataGatherer that produced
// the data we're checking can only read tar files, which are allowed under
// both APTrust and BTR specs. So the fact that we have metadata means we
// read a tar file, and we can report the serialization was OK.
func (v *MetadataValidator) SerializationOk() bool {
	formatsAllowed := v.Profile.AcceptSerialization
	formatReceived := v.IngestObject.Serialization
	if v.Profile.Serialization == "forbidden" && formatReceived != "" {
		v.AddError("BagIt profile forbids serialization but bag is serialized in %s format", formatReceived)
		return false
	}
	if v.Profile.Serialization == "required" && formatReceived == "" {
		v.AddError("Bag is not serialized, but profile requires serialization in one of the following formats: %s", strings.Join(formatsAllowed, ", "))
		return false
	}
	if v.Profile.Serialization != "required" && formatReceived == "" {
		return true
	}
	ok := true
	if !util.StringListContains(formatsAllowed, formatReceived) {
		v.AddError("BagIt profile does not allow serialization format %s", formatReceived)
		ok = false
	}
	return ok
}

func (v *MetadataValidator) BagItVersionOk() bool {
	tag := v.IngestObject.GetTag("bagit.txt", "BagIt-Version")
	if tag == nil || tag.Value == "" {
		v.AddError("Missing required tag bagit.txt/BagIt-Version.")
		return false
	}
	ok := util.StringListContains(v.Profile.AcceptBagItVersion, tag.Value)
	if ok == false {
		v.AddError("BagIt-Version %s is not permitted in BagIt profile %s.",
			tag.Value, v.Profile.BagItProfileInfo.BagItProfileIdentifier)
	}
	return ok
}

// Ideally, we'd fully implement this so that it checks the contents
// of fetch.txt, but since we don't ever plan on allowing fetch.txt
// files, we're going to skip full validation.
func (v *MetadataValidator) FetchTxtOk() bool {
	if v.Profile.AllowFetchTxt == true {
		return true
	}
	ok := true
	if v.IngestObject.HasFetchTxt {
		v.AddError("Bag has fetch.txt file which profile does not allow")
		ok = false
	}
	return ok
}

func (v *MetadataValidator) ManifestsAllowedOk() bool {
	return v.ValidateAllowed(
		"manifest",
		v.Profile.ManifestsAllowed,
		v.IngestObject.Manifests)
}

func (v *MetadataValidator) ManifestsRequiredOk() bool {
	ok := v.ValidateRequired(
		"manifest",
		v.Profile.ManifestsRequired,
		v.IngestObject.Manifests)
	// BagIt-Profiles spec 1.3+ can specify multiple ManifestsAllowed
	// with no ManifestsRequired, meaning the bagger is free to choose
	// any algorithm from ManifestsAllowed. However, the bag MUST have
	// at least one payload manifest to be valid.
	if ok && len(v.IngestObject.Manifests) == 0 {
		v.AddError("Bag must have at least one payload manifest")
		ok = false
	}
	return ok
}

// TODO: We actually have to do pattern matching for this.
// E.g. If TagFilesAllowed says "custom-tags/*" then we
// have to make sure tag files start with "custom-tags/".
// APTrust and BTR allow any tag files, so we're skipping
// this for now.
func (v *MetadataValidator) TagFilesAllowedOk() bool {
	return v.ValidateAllowed(
		"tag file",
		v.Profile.TagFilesAllowed,
		v.IngestObject.TagFiles)
}

func (v *MetadataValidator) TagManifestsAllowedOk() bool {
	return v.ValidateAllowed(
		"tag manifest",
		v.Profile.TagManifestsAllowed,
		v.IngestObject.TagManifests)
}

func (v *MetadataValidator) TagManifestsRequiredOk() bool {
	return v.ValidateRequired(
		"tag manifest",
		v.Profile.TagManifestsRequired,
		v.IngestObject.TagManifests)
}

func (v *MetadataValidator) HasAllRequiredTags() bool {
	ok := true
	for _, tagDef := range v.Profile.Tags {
		tag := v.IngestObject.GetTag(tagDef.TagFile, tagDef.TagName)
		if tagDef.Required && tag == nil {
			v.AddError("Required tag %s in file %s is missing",
				tagDef.TagName, tagDef.TagFile)
			ok = false
		}
	}
	return ok
}

func (v *MetadataValidator) ExistingTagsOk() bool {
	ok := true
	for _, tag := range v.IngestObject.Tags {
		if !v.TagOk(tag) {
			ok = false
		}
	}
	return ok
}

func (v *MetadataValidator) TagOk(tag *bagit.Tag) bool {
	ok := true
	tagDef := v.Profile.GetTagDef(tag.TagFile, tag.TagName)
	// If no tag def, the tag is allowed and has no restrictions.
	// If there is a tag def, validate...
	if tagDef != nil {
		if tagDef.Required && tag.Value == "" {
			v.AddError("In file %s, required tag %s has no value",
				tag.TagFile, tag.TagName)
			ok = false
		} else if !tagDef.IsLegalValue(tag.Value) {
			v.AddError("In file %s, tag %s has illegal value '%s'",
				tag.TagFile, tag.TagName, tag.Value)
			ok = false
		}
	}
	return ok
}

func (v *MetadataValidator) IngestFilesOk() bool {
	ok := true
	nextOffset := uint64(0)
	batchSize := int64(100)
	var fileMap map[string]*service.IngestFile
	var err error
	for {
		fileMap, nextOffset, err = v.Context.RedisClient.GetBatchOfFileKeys(
			v.WorkItemID, nextOffset, batchSize)
		if err != nil {
			v.AddError("Internal error during validation: "+
				"could not get file info from cache: %s. "+
				"This is a system error, not a problem with the bag.",
				err.Error())
			ok = false
			break
		}
		for _, ingestFile := range fileMap {
			if !v.IngestFileOk(ingestFile) {
				ok = false
			}
		}
		// When scanning hash keys, redis returns cursor value
		// of zero after it has iterated the entire collection.
		if nextOffset == 0 {
			break
		}
	}
	return ok
}

// IngestFileOk returns true if the filename consists entirely of legal
// characters and the checksum of the file matches what's in the manifest.
// Note, however, that the BagIt spec says some tag files can be excluded
// from the tag manifests. In those cases, we may validate only the filename.
func (v *MetadataValidator) IngestFileOk(f *service.IngestFile) bool {
	ok := true
	_, err := f.IdentifierIsLegal()
	if err != nil {
		v.AddError(err.Error())
		ok = false
	}
	if !v.ValidateChecksums(f, constants.FileTypeManifest, v.IngestObject.Manifests) {
		ok = false
	}
	if !v.ValidateChecksums(f, constants.FileTypeTagManifest, v.IngestObject.TagManifests) {
		ok = false
	}
	return ok
}

func (v *MetadataValidator) ValidateChecksums(f *service.IngestFile, manifestType string, algorithms []string) bool {
	ok := true
	for _, alg := range v.IngestObject.Manifests {
		manifestIsPresent := false
		manifestName := ""
		switch manifestType {
		case constants.FileTypeManifest:
			manifestName = fmt.Sprintf("manifest-%s.txt", alg)
			manifestIsPresent = util.StringListContains(v.IngestObject.Manifests, alg)
		case constants.FileTypeTagManifest:
			manifestName = fmt.Sprintf("tagmanifest-%s.txt", alg)
			manifestIsPresent = util.StringListContains(v.IngestObject.TagManifests, alg)
		default:
			// Panic, because this is entirely in the developer's control.
			msg := fmt.Sprintf("Invalid manifest type: %s", manifestType)
			panic(msg)
		}
		// manifestIsPresent mainly pertains to tag manifests, which are
		// entirely optional. We don't want to try to validate a checksum
		// against a manifest that isn't there.
		if manifestIsPresent {
			_, err := f.ChecksumsMatch(manifestName)
			if err != nil {
				v.AddError(err.Error())
				ok = false
			}
		}
	}
	return ok
}

func (v *MetadataValidator) AddError(format string, a ...interface{}) {
	if len(v.Errors) < constants.MaxValidationErrors {
		v.Errors = append(v.Errors, fmt.Sprintf(format, a...))
	} else if len(v.Errors) == constants.MaxValidationErrors {
		v.Errors = append(v.Errors, "Too many errors")
	}
}

func (v *MetadataValidator) ClearErrors() {
	v.Errors = make([]string, 0)
}

func (v *MetadataValidator) AnythingGoes(list []string) bool {
	// Spec at https://bagit-profiles.github.io/bagit-profiles-specification/
	// says if [Tag]ManifestsAllowed is empty, any [tag]manifests are allowed.
	//
	// There's actually more nuance than that, as any items in a required
	// list must also be in an allowed list. We should validate that when
	// validating the profile, not here.
	return list == nil || len(list) == 0 || util.StringListContains(list, "*")
}

func (v *MetadataValidator) ValidateAllowed(filetype string, allowedInProfile, presentInBag []string) bool {
	if v.AnythingGoes(allowedInProfile) {
		return true
	}
	return v.RecordIllegals(filetype, allowedInProfile, presentInBag)
}

func (v *MetadataValidator) ValidateRequired(filetype string, requiredByProfile, presentInBag []string) bool {
	if v.AnythingGoes(requiredByProfile) {
		return true
	}
	return v.RecordMissing(filetype, requiredByProfile, presentInBag)
}

func (v *MetadataValidator) RecordIllegals(filetype string, allowedInProfile, presentInBag []string) bool {
	ok := true
	for _, file := range presentInBag {
		if !util.StringListContains(allowedInProfile, file) {
			v.AddError("Bag contains illegal %s '%s'", filetype, file)
			ok = false
		}
	}
	return ok
}

func (v *MetadataValidator) RecordMissing(filetype string, requiredByProfile, presentInBag []string) bool {
	ok := true
	for _, file := range requiredByProfile {
		if !util.StringListContains(presentInBag, file) {
			v.AddError("Bag is missing required %s '%s'", filetype, file)
			ok = false
		}
	}
	return ok
}
