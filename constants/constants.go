package constants

import (
	"fmt"
)

const (
	AccessConsortia            = "consortia"
	AccessInstitution          = "institution"
	AccessRestricted           = "restricted"
	ActionDelete               = "Delete"
	ActionFixityCheck          = "Fixity Check"
	ActionGlacierRestore       = "Glacier Restore"
	ActionIngest               = "Ingest"
	ActionRestoreFile          = "Restore File"
	ActionRestoreObject        = "Restore Object"
	AdminAPIPrefix             = "admin-api"
	AlgMd5                     = "md5"
	AlgSha1                    = "sha1"
	AlgSha256                  = "sha256"
	AlgSha512                  = "sha512"
	AWSBucketPrefix            = "https://s3.amazonaws.com/"
	BagItProfileBTR            = "btr-v1.0.json"
	BagItProfileDefault        = "aptrust-v2.3.json"
	BagRestorer                = "bag_restorer"
	BTRProfileIdentifier       = "https://github.com/dpscollaborative/btr_bagit_profile/releases/download/1.0/btr-bagit-profile.json"
	DefaultAccess              = AccessInstitution
	DefaultProfileIdentifier   = "https://raw.githubusercontent.com/APTrust/preservation-services/master/profiles/aptrust-v2.3.json"
	Deleter                    = "deleter"
	EmptyUUID                  = "00000000-0000-0000-0000-000000000000"
	EventAccessAssignment      = "access assignment"
	EventAgentMinioV4          = 1
	EventAgentStringMinioV4    = "https://github.com/minio/minio-go v4"
	EventAgentMinioV5          = 2
	EventAgentStringMinioV5    = "https://github.com/minio/minio-go v5"
	EventAgentMinioV6          = 3
	EventAgentStringMinioV6    = "https://github.com/minio/minio-go v6"
	EventAgentMinioV7          = 4
	EventAgentStringMinioV7    = "https://github.com/minio/minio-go v7"
	EventAgentPreserv          = 5
	EventAgentStringPreserv    = "https://github.com/APTrust/preservation-services"
	EventAgentPreservAlt       = 6
	EventAgentStringPreservAlt = "APTrust preservation services"
	EventAgentUUID             = 7
	EventAgentStringUUID       = "http://github.com/google/uuid"
	EventAgentSHA256           = 8
	EventAgentStringSHA256     = "http://golang.org/pkg/crypto/sha256/"
	EventAgentMD5              = 9
	EventAgentStringMD5        = "http://golang.org/pkg/crypto/md5/"
	EventAgentTest             = 10
	EventAgentStringTest       = "Registry Unit Test"
	EventAgentTestAlt          = 11
	EventAgentStringTestAlt    = "Maxwell Smart"
	EventAgentFixture          = 12
	EventAgentStringFixture    = "https://github.com/APTrust/exchange"
	EventAgentUUIDPast         = 13
	EventAgentStringUUIDPast   = "http://github.com/satori/go.uuid"
	EventAgentNu7Hatch         = 14
	EventAgentStringNu7Hatch   = "http://github.com/nu7hatch/gouuid"
	EventAgentBagman           = 15
	EventAgentStringBagman     = "https://github.com/APTrust/bagman"
	EventAgentAwsSdk           = 16
	EventAgentStringAwsSdk     = "https://github.com/aws/aws-sdk-go"
	EventAgentGoamz            = 17
	EventAgentStringGoamz      = "https://github.com/crowdmob/goamz"
	EventAgentMarcel           = 18
	EventAgentStringMarcel     = "https://github.com/marcel/aws-s3/tree/master"
	EventAgentUuidHttps        = 19
	EventAgentStringUuidHttps  = "https://github.com/satori/go.uuid"
	EventAgentLaunchpad        = 20
	EventAgentStringLaunchpad  = "https://launchpad.net/goamz"
	EventAgentAudit            = 21
	EventAgentStringAudit      = "https://github.com/APTrust/auditing/blob/1.0/cleanup_001.py"
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
	EventObjectPreserv         = 1
	EventObjectStringPreserv   = "APTrust preservation services"
	EventObjectMinio           = 2
	EventObjectStringMinio     = "Minio S3 client"
	EventObjectMinioAlt        = 3
	EventObjectStringMinioAlt  = "Minio S3 library"
	EventObjectPreMinio        = 4
	EventObjectStringPreMinio  = "preservation-services + Minio S3 client"
	EventObjectUUIDMinio       = 5
	EventObjectStringUUIDMinio = "Go uuid library + Minio S3 library"
	EventObjectSHA256          = 6
	EventObjectStringSHA256    = "Go language crypto/sha256"
	EventObjectMD5             = 7
	EventObjectStringMD5       = "Go language crypto/md5"
	EventObjectTest            = 8
	EventObjectStringTest      = "scissors"
	EventObjectExchange        = 9
	EventObjectStringExchange  = "APTrust exchange/ingest processor"
	EventObjectTestAlt         = 10
	EventObjectStringTestAlt   = "Fake event object"
	EventObjectFixS3           = 11
	EventObjectStringFixS3     = "APTrust Go Exchange + Amazon S3 client"
	EventObjectFixSHA          = 12
	EventObjectStringFixSHA    = "SHA-256 thingy"
	EventObjectFixExch         = 13
	EventObjectStringFixExch   = "Exchange ingest code"
	EventObjectFixDelete       = 14
	EventObjectStringFixDelete = "Deleterbot code"
	EventObjectBagman          = 15
	EventObjectStringBagman    = "APTrust bagman"
	EventObjectBagProc         = 16
	EventObjectStringBagProc   = "APTrust bag processor"
	EventObjectExchOld         = 17
	EventObjectStringExchOld   = "APTrust exchange"
	EventObjectExDelete        = 18
	EventObjectStringExDelete  = "APTrust Exchange apt_delete service"
	EventObjectExIngest        = 19
	EventObjectStringExIngest  = "APTrust Exchange ingest services"
	EventObjectExUUID          = 20
	EventObjectStringExUUID    = "APTrust exchange using Satori go.uuid"
	EventObjectAwsClient       = 21
	EventObjectStringAwsClient = "AWS Go SDK S3 client"
	EventObjectAwsLib          = 22
	EventObjectStringAwsLib    = "AWS Go SDK S3 Library"
	EventObjectBagmanGo        = 23
	EventObjectStringBagmanGo  = "bagman + goamz s3 client"
	EventObjectExAws           = 24
	EventObjectStringExAws     = "exchange + AWS Go SDK S3 client"
	EventObjectExGoamz         = 25
	EventObjectStringExGoamz   = "exchange + goamz S3 client"
	EventObjectGoamz           = 26
	EventObjectStringGoamz     = "goamz S3 client"
	EventObjectGoamzAlt        = 27
	EventObjectStringGoamzAlt  = "Goamz S3 Client"
	EventObjectCryptoh         = 28
	EventObjectStringCryptoh   = "Go language cryptohash"
	EventObjectMD5Past         = 29
	EventObjectStringMD5Past   = "Go crypto/md5"
	EventObjectDPN             = 30
	EventObjectStringDPN       = "Go uuid library + APTrust DPN services"
	EventObjectUuidAws         = 31
	EventObjectStringUuidAws   = "Go uuid library + AWS Go SDK S3 library"
	EventObjectUuidGoamz       = 32
	EventObjectStringUuidGoamz = "Go uuid library + goamz S3 library"
	EventObjectRuby            = 33
	EventObjectStringRuby      = "Ruby aws-s3 gem"
	EventObjectAudit           = 34
	EventObjectStringAudit     = "APTrust audit and cleanup scripts for audit_001"
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
	LineSeparator              = rune(0x2028)
	MatchTypeContainer         = "container"
	MatchTypeExtension         = "extension"
	MatchTypeSignature         = "signature"
	MaxS3RequestSize           = int64(5497558138880) // 5TB - Max size for single PUT/POST/GET request
	MaxServerSideCopySize      = int64(5368709120)    // 5GB
	MaxValidationErrors        = 30
	MemberAPIPrefix            = "member-api" // DART uses this
	NarrowNonBreakingSpace     = " "
	OutcomeFailure             = "Failure"
	OutcomeSuccess             = "Success"
	RegionAWSUSEast1           = "us-east-1"    // AWS Virginia
	RegionAWSUSEast2           = "us-east-2"    // AWS Ohio
	RegionAWSUSWest1           = "us-west-1"    // AWS California
	RegionAWSUSWest2           = "us-west-2"    // AWS Oregon
	RegionWasabiUSCentral1     = "us-central-1" // Plano, TX
	RegionWasabiUSEast1        = "us-east-1"    // Wasabi Virginia
	RegionWasabiUSEast2        = "us-east-2"    // Wasabi Virginia (2)
	RegionWasabiUSWest1        = "us-west-1"    // Wasabi Oregon
	RestorationBaggingSoftware = "APTrust preservation-services restoration bagger"
	RestorationSourceGlacier   = "glacier"
	RestorationSourceS3        = "s3"
	RestorationTypeFile        = "file"
	RestorationTypeObject      = "object"
	S3ClientName               = "https://github.com/minio/minio-go v7"
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
	StorageProviderWasabiOR    = "Wasabi-OR"
	StorageProviderWasabiTX    = "Wasabi-TX"
	StorageProviderWasabiVA    = "Wasabi-VA"
	StorageStandard            = "Standard"
	StorageWasabiOR            = "Wasabi-OR"
	StorageWasabiTX            = "Wasabi-TX"
	StorageWasabiVA            = "Wasabi-VA"
	TopicDelete                = "delete_item"
	TopicE2EDelete             = "e2e_deletion_post_test"
	TopicE2EFixity             = "e2e_fixity_post_test"
	TopicE2EIngest             = "e2e_ingest_post_test"
	TopicE2EReingest           = "e2e_reingest_post_test"
	TopicE2ERestore            = "e2e_restoration_post_test"
	TopicFileRestore           = "restore_file"
	TopicFixity                = "fixity_check"
	TopicGlacierRestore        = "restore_glacier"
	TopicObjectRestore         = "restore_object"
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

// SupportedManifestAlgorithms lists the digest algorithms we support
// for ingest.
var SupportedManifestAlgorithms []string = []string{
	AlgMd5,
	AlgSha1,
	AlgSha256,
	AlgSha512,
}

// APTrustRestorationAlgorithms lists the digest algorithms to use
// when restoring bags in APTrust format.
var APTrustRestorationAlgorithms []string = []string{
	AlgMd5,
	AlgSha256,
}

// BTRRestorationAlgorithms lists the digest algorithms to use
// when restoring bags in BTR format.
var BTRRestorationAlgorithms []string = []string{
	AlgSha1,
	AlgSha256,
	AlgSha512,
}

var StorageProviders = []string{
	StorageProviderAWS,
	StorageProviderLocal,
	StorageProviderWasabiOR,
	StorageProviderWasabiVA,
}

var WasabiStorageProviders = []string{
	StorageProviderWasabiOR,
	StorageProviderWasabiVA,
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
	StatusSuspended,
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

var ManifestTypes = []string{
	FileTypeManifest,
	FileTypeTagManifest,
}

// TopicFor returns the NSQ topic for the specified action and stage.
// Param fileIdentifier may be GenericFile.Identifier or an empty string.
func TopicFor(action, stage, fileIdentifier string) (topic string, err error) {
	if action == ActionIngest {
		//topic = ingestTopics[action+stage]
		for _, s := range IngestStages {
			if s.Name == stage {
				topic = s.NSQTopic
			}
		}
	} else if action == ActionFixityCheck {
		topic = TopicFixity
	} else if action == ActionRestoreFile {
		topic = TopicFileRestore
	} else if action == ActionRestoreObject {
		topic = TopicObjectRestore
	} else if action == ActionGlacierRestore {
		topic = TopicGlacierRestore
	} else if action == ActionDelete {
		topic = TopicDelete
	}
	if topic == "" {
		err = fmt.Errorf("No NSQ topic for %s/%s", action, stage)
	}
	return topic, err
}

func IngestStageFor(topic string) (stage string, err error) {
	for _, s := range IngestStages {
		if s.NSQTopic == topic {
			stage = s.Name
		}
	}
	if stage == "" {
		err = fmt.Errorf("Cannot map NSQ topic %s to any ingest stage", topic)
	}
	return stage, err
}
