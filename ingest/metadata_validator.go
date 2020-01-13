package ingest

import (
	"fmt"
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
	"github.com/APTrust/preservation-services/util"
)

type MetadataValidator struct {
	Context      *common.Context
	Errors       []string
	IngestObject *service.IngestObject
	Profile      *bagit.BagItProfile
	WorkItemId   int
}

func NewMetadataValidator(context *common.Context, profile *bagit.BagItProfile, ingestObject *service.IngestObject, workItemId int) *MetadataValidator {
	return &MetadataValidator{
		Context:      context,
		Errors:       make([]string, 0),
		IngestObject: ingestObject,
		Profile:      profile,
		WorkItemId:   workItemId,
	}
}

func (v *MetadataValidator) IsValid() bool {
	return true
}

func (v *MetadataValidator) BagItVersionOk() bool {
	tag := v.IngestObject.GetTag("bagit.txt", "BagIt-Version")
	if tag == nil || tag.Value == "" {
		v.AddError("Missing required tag bag-info.txt/BagIt-Version.")
		return false
	}
	ok := util.StringListContains(v.Profile.AcceptBagItVersion, tag.Value)
	if ok == false {
		v.AddError("BagIt-Version %s is not permitted in BagIt profile %s.",
			tag.Value, v.Profile.BagItProfileInfo.BagItProfileIdentifier)
	}
	return ok
}

// Technically, we should check this. But the MetadataGatherer that produced
// the data we're checking can only read tar files, which are allowed under
// both APTrust and BTR specs. So the fact that we have metadata means we
// read a tar file, and we can report the serialization was OK.
func (v *MetadataValidator) SerializationOk() bool {
	return true
}

func (v *MetadataValidator) FetchTxtOk() bool {
	if v.Profile.AllowFetchTxt == true {
		return true
	}
	return !v.IngestObject.HasFetchTxt
}

func (v *MetadataValidator) ManifestsAllowedOk() bool {
	return v.ValidateAllowed(
		"manifest",
		v.Profile.ManifestsAllowed,
		v.IngestObject.Manifests)
}

func (v *MetadataValidator) ManifestsRequiredOk() bool {
	return v.ValidateRequired(
		"manifest",
		v.Profile.ManifestsRequired,
		v.IngestObject.Manifests)
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
		if tag == nil {
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
	tagDef := v.Profile.GetTagDef(tag.SourceFile, tag.Label)
	// If no tag def, the tag is allowed and has no restrictions.
	// If there is a tag def, validate...
	if tagDef != nil {
		if tagDef.Required && tag.Value == "" {
			v.AddError("In file %s, required tag %s has no value",
				tag.SourceFile, tag.Label)
			ok = false
		} else if !tagDef.IsLegalValue(tag.Value) {
			v.AddError("In file %s, tag %s has illegal value '%s'",
				tag.SourceFile, tag.Label, tag.Value)
		}
	}
	return ok
}

func (v *MetadataValidator) IngestFileOk(f *service.IngestFile) bool {
	// Make sure checksums match
	// Make sure name is legal (i.e. no control chars or other trash)
	return true
}

func (v *MetadataValidator) AddError(format string, a ...interface{}) {
	if len(v.Errors) < 30 {
		v.Errors = append(v.Errors, fmt.Sprintf(format, a...))
	} else if len(v.Errors) == 30 {
		v.Errors = append(v.Errors, "Too many errors")
	}
}

func (v *MetadataValidator) AnythingGoes(list []string) bool {
	return list == nil || len(list) == 0 || util.StringListContains(list, "*")
}

func (v *MetadataValidator) ValidateAllowed(filetype string, allowedInProfile, presentInBag []string) bool {
	if v.AnythingGoes(allowedInProfile) {
		return true
	}
	return v.RecordIllegals(filetype, allowedInProfile, presentInBag)
}

func (v *MetadataValidator) ValidateRequired(filetype string, allowedInProfile, presentInBag []string) bool {
	if v.AnythingGoes(allowedInProfile) {
		return true
	}
	return v.RecordIllegals(filetype, allowedInProfile, presentInBag)
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
