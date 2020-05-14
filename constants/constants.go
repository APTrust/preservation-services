package constants

const (
	AccessConsortia            = "consortia"
	AccessInstitution          = "institution"
	AccessRestricted           = "restricted"
	ActionDelete               = "Delete"
	ActionFixityCheck          = "Fixity Check"
	ActionGlacierRestore       = "Glacier Restore"
	ActionIngest               = "Ingest"
	ActionRestore              = "Restore"
	AlgMd5                     = "md5"
	AlgSha256                  = "sha256"
	AlgSha512                  = "sha512"
	BagItProfileBTR            = "btr-v1.0.json"
	BagItProfileDefault        = "aptrust-v2.2.json"
	DefaultAccess              = AccessInstitution
	DefaultProfileIdentifier   = "https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.2.json"
	EmptyUUID                  = "00000000-0000-0000-0000-000000000000"
	EventAccessAssignment      = "access assignment"
	EventCapture               = "capture"
	EventCompression           = "compression"
	EventCreation              = "creation"
	EventDeaccession           = "deaccession"
	EventDecompression         = "decompression"
	EventDecryption            = "decryption"
	EventDeletion              = "deletion"
	EventDigestCalculation     = "message digest calculation"
	EventFixityCheck           = "fixity check"
	EventIdentifierAssignment  = "identifier assignment"
	EventIngestion             = "ingestion"
	EventMigration             = "migration"
	EventNormalization         = "normalization"
	EventReplication           = "replication"
	EventSignatureValidation   = "digital signature validation"
	EventValidation            = "validation"
	EventVirusCheck            = "virus check"
	FileTypeFetchTxt           = "fetch.txt"
	FileTypeManifest           = "manifest"
	FileTypePayload            = "payload_file"
	FileTypeTag                = "tag_file"
	FileTypeTagManifest        = "tag_manifest"
	FmtIdExtMap                = "ext map"
	FmtIdSiegfried             = "siegfried"
	IdTypeStorageURL           = "url"
	IdTypeBagAndPath           = "bag/filepath"
	IngestPreFetch             = "ingest01_prefetch"
	IngestValidation           = "ingest02_bag_validation"
	IngestReingestCheck        = "ingest03_reingest_check"
	IngestStaging              = "ingest04_staging"
	IngestFormatIdentification = "ingest05_format_identification"
	IngestStorage              = "ingest06_storage"
	IngestStorageValidation    = "ingest07_storage_validation"
	IngestRecord               = "ingest08_record"
	IngestCleanup              = "ingest09_cleanup"
	MatchTypeContainer         = "container"
	MatchTypeExtension         = "extension"
	MatchTypeSignature         = "signature"
	MaxValidationErrors        = 30
	OutcomeFailure             = "Failure"
	OutcomeSuccess             = "Success"
	RegionAWSUSEast1           = "us-east-1" // AWS Virginia
	RegionAWSUSEast2           = "us-east-2" // AWS Ohio
	RegionAWSUSWest1           = "us-west-1" // AWS California
	RegionAWSUSWest2           = "us-west-2" // AWS Oregon
	RegionWasabiUSEast1        = "us-east-1" // Wasabi Virginia
	RegionWasabiUSEast2        = "us-east-2" // Wasabi Virginia (2)
	RegionWasabiUSWest1        = "us-west-1" // Wasabi Oregon
	S3ClientName               = "https://github.com/minio/minio-go v6"
	SourceIngest               = "ingest"
	SourceManifest             = "manifest"
	SourceRegistry             = "registry"
	SourceTagManifest          = "tag_manifest"
	StageAvailableInS3         = "Available in S3"
	StageCleanup               = "Cleanup"
	StageCopyToStaging         = "Copy To Staging"
	StageFormatIdentification  = "Format Identification"
	StageFetch                 = "Fetch"
	StagePackage               = "Package"
	StageReceive               = "Receive"
	StageRecord                = "Record"
	StageReingestCheck         = "Reingest Check"
	StageRequested             = "Requested"
	StageResolve               = "Resolve"
	StageRestoring             = "Restoring"
	StageStorageValidation     = "Storage Validation"
	StageStore                 = "Store"
	StageUnpack                = "Unpack"
	StageValidate              = "Validate"
	StateActive                = "A"
	StateDeleted               = "D"
	StatusCancelled            = "Cancelled"
	StatusFailed               = "Failed"
	StatusPending              = "Pending"
	StatusStarted              = "Started"
	StatusSuccess              = "Success"
	StatusSuspended            = "Suspended"
	StorageClassStandard       = "Standard"
	StorageClassIntelligent    = "Intelligent-Tiering"
	StorageClassStandardIA     = "Standard IA"
	StorageClassOneZoneIA      = "One Zone IA"
	StorageClassGlacier        = "Glacier"
	StorageClassGlacierDeep    = "Glacier Deep Archive"
	StorageClassWasabi         = "Wasabi"
	StorageGlacierDeepOH       = "Glacier-Deep-OH"
	StorageGlacierDeepOR       = "Glacier-Deep-OR"
	StorageGlacierDeepVA       = "Glacier-Deep-VA"
	StorageGlacierOH           = "Glacier-OH"
	StorageGlacierOR           = "Glacier-OR"
	StorageGlacierVA           = "Glacier-VA"
	StorageProviderAWS         = "AWS"
	StorageProviderLocal       = "Local"
	StorageProviderWasabi      = "Wasabi"
	StorageStandard            = "Standard"
	StorageWasabiOR            = "Wasabi-OR"
	StorageWasabiVA            = "Wasabi-VA"
	TypeFile                   = "GenericFile"
	TypeObject                 = "IntellectualObject"
)

var IngestOpNames []string = []string{
	IngestPreFetch,
	IngestValidation,
	IngestReingestCheck,
	IngestStaging,
	IngestFormatIdentification,
	IngestStorage,
	IngestStorageValidation,
	IngestRecord,
	IngestCleanup,
}

var IngestTopicNames = IngestOpNames

var PreferredAlgsInOrder []string = []string{
	AlgSha512,
	AlgSha256,
	AlgMd5,
}

var StorageProviders = []string{
	StorageProviderAWS,
	StorageProviderLocal,
	StorageProviderWasabi,
}

var StorageOptions = []string{
	StorageGlacierDeepOH,
	StorageGlacierDeepOR,
	StorageGlacierDeepVA,
	StorageGlacierOH,
	StorageGlacierOR,
	StorageGlacierVA,
	StorageStandard,
	StorageWasabiOR,
	StorageWasabiVA,
}

var CompletedStatusValues = []string{
	StatusCancelled,
	StatusFailed,
	StatusSuccess,
}

var IncompleteStatusValues = []string{
	StatusPending,
	StatusStarted,
}

var LateStagesOfIngest = []string{
	IngestStorage,
	IngestStorageValidation,
	IngestRecord,
	IngestCleanup,
}

var TypeNames = []string{
	TypeFile,
	TypeObject,
}
