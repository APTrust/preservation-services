package constants

const (
	AccessConsortia           = "consortia"
	AccessInstitution         = "institution"
	AccessRestricted          = "restricted"
	AlgMd5                    = "md5"
	AlgSha256                 = "sha256"
	AlgSha512                 = "sha512"
	BagItProfileBTR           = "btr-v1.0.json"
	BagItProfileDefault       = "aptrust-v2.2.json"
	EmptyUUID                 = "00000000-0000-0000-0000-000000000000"
	FileTypeFetchTxt          = "fetch.txt"
	FileTypeManifest          = "manifest"
	FileTypePayload           = "payload_file"
	FileTypeTag               = "tag_file"
	FileTypeTagManifest       = "tag_manifest"
	OpIngestCharacterize      = "Ingest - File Characterization"
	OpIngestCheckForUpdate    = "Ingest - Check for Update"
	OpIngestCleanup           = "Ingest - Cleanup"
	OpIngestGatherMeta        = "Ingest - Gather Metadata"
	OpIngestPreserve          = "Ingest - Copy to Preservation"
	OpIngestRecord            = "Ingest - Record in Registry"
	OpIngestReplicate         = "Ingest - Copy to Replication"
	OpIngestStage             = "Ingest - Copy to Staging"
	OpIngestStorageValidation = "Ingest - Storage Validation"
	OpIngestValidate          = "Ingest - Validation"
	OutcomeFailure            = "Failure"
	OutcomeSuccess            = "Success"
	S3ClientAWS               = "AWS"
	S3ClientWasabi            = "Wasabi"
	SourceIngest              = "ingest"
	SourceManifest            = "manifest"
	SourceRegistry            = "registry"
	SourceTagManifest         = "tag_manifest"
	StatusCancelled           = "Cancelled"
	StatusFailed              = "Failed"
	StatusPending             = "Pending"
	StatusStarted             = "Started"
	StatusSuccess             = "Success"
)

var DigestAlgorithms []string = []string{
	AlgMd5,
	AlgSha256,
	AlgSha512,
}
