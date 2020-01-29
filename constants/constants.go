package constants

const (
	AccessConsortia           = "consortia"
	AccessInstitution         = "institution"
	AccessRestricted          = "restricted"
	ActionDelete              = "Delete"
	ActionFixityCheck         = "Fixity Check"
	ActionGlacierRestore      = "Glacier Restore"
	ActionIngest              = "Ingest"
	ActionRestore             = "Restore"
	AlgMd5                    = "md5"
	AlgSha256                 = "sha256"
	AlgSha512                 = "sha512"
	BagItProfileBTR           = "btr-v1.0.json"
	BagItProfileDefault       = "aptrust-v2.2.json"
	EmptyUUID                 = "00000000-0000-0000-0000-000000000000"
	EventAccessAssignment     = "access assignment"
	EventCapture              = "capture"
	EventCompression          = "compression"
	EventCreation             = "creation"
	EventDeaccession          = "deaccession"
	EventDecompression        = "decompression"
	EventDecryption           = "decryption"
	EventDeletion             = "deletion"
	EventDigestCalculation    = "message digest calculation"
	EventFixityCheck          = "fixity check"
	EventIdentifierAssignment = "identifier assignment"
	EventIngestion            = "ingestion"
	EventMigration            = "migration"
	EventNormalization        = "normalization"
	EventReplication          = "replication"
	EventSignatureValidation  = "digital signature validation"
	EventValidation           = "validation"
	EventVirusCheck           = "virus check"
	FileTypeFetchTxt          = "fetch.txt"
	FileTypeManifest          = "manifest"
	FileTypePayload           = "payload_file"
	FileTypeTag               = "tag_file"
	FileTypeTagManifest       = "tag_manifest"
	MaxValidationErrors       = 30
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
	StageAvailableInS3        = "Available in S3"
	StageCleanup              = "Cleanup"
	StageFetch                = "Fetch"
	StagePackage              = "Package"
	StageReceive              = "Receive"
	StageRecord               = "Record"
	StageRequested            = "Requested"
	StageResolve              = "Resolve"
	StageRestoring            = "Restoring"
	StageStore                = "Store"
	StageUnpack               = "Unpack"
	StageValidate             = "Validate"
	StatusCancelled           = "Cancelled"
	StatusFailed              = "Failed"
	StatusPending             = "Pending"
	StatusStarted             = "Started"
	StatusSuccess             = "Success"
	StorageGlacierDeepOH      = "Glacier-Deep-OH"
	StorageGlacierDeepOR      = "Glacier-Deep-OR"
	StorageGlacierDeepVA      = "Glacier-Deep-VA"
	StorageGlacierOH          = "Glacier-OH"
	StorageGlacierOR          = "Glacier-OR"
	StorageGlacierVA          = "Glacier-VA"
	StorageStandard           = "Standard"
	StorageWasabiOR           = "Wasabi-OR"
	StorageWasabiVA           = "Wasabi-VA"
)

var DigestAlgorithms []string = []string{
	AlgMd5,
	AlgSha256,
	AlgSha512,
}
