package ingest

import (
	"github.com/APTrust/preservation-services/bagit"
	"github.com/APTrust/preservation-services/models/common"
	"github.com/APTrust/preservation-services/models/service"
)

type MetadataValidator struct {
	Context      *common.Context
	Errors       []string
	IngestObject *service.IngestObject
	Profile      *bagit.BagItProfile
}

func NewMetadataValidator(context *common.Context, profile *bagit.BagItProfile, ingestObject *service.IngestObject) *MetadataValidator {
	return &MetadataValidator{
		Context: context,
		Profile: profile,
		Errors:  make([]string, 0),
	}
}

func (v *MetadataValidator) IsValid() bool {
	return true
}

func (v *MetadataValidator) BagItVersionOk() bool {
	return true
}

func (v *MetadataValidator) SerializationOk() bool {
	return true
}

func (v *MetadataValidator) FetchTxtOk() bool {
	return true
}

func (v *MetadataValidator) ManifestsAllowedOk() bool {
	return true
}

func (v *MetadataValidator) ManifestsRequiredOk() bool {
	return true
}

func (v *MetadataValidator) TagFilesAllowedOk() bool {
	return true
}

func (v *MetadataValidator) TagManifestsAllowedOk() bool {
	return true
}

func (v *MetadataValidator) TagManifestsRequiredOk() bool {
	return true
}

func (v *MetadataValidator) TagsOk() bool {
	return true
}

func (v *MetadataValidator) TagOk(tag *bagit.Tag) bool {
	return true
}
