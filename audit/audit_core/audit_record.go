package audit_core

import (
	"strconv"
	"strings"
	"time"
)

const (
	QuickMatch = "Quick Match"
	FullFixity = "Full Fixity"
)

type AuditRecord struct {
	CheckStartedAt            time.Time
	CheckCompletedAt          time.Time
	CheckPassed               bool
	Method                    string // "Quick Match" or "Full Fixity"
	GenericFileID             int64
	GenericFileIdentifier     string
	GenericFileCreatedAt      time.Time
	GenericFileUpdatedAt      time.Time
	IsGlacierOnlyFile         bool
	NeedsGlacierFixityCheck   bool
	ReasonForCheck            string
	RegistryMd5               string
	RegistrySha256            string
	RegistrySize              int64
	S3Etag                    string
	S3MetaMd5                 string
	S3MetaSha256              string
	S3MetaPathInBag           string
	S3MetaBagName             string
	S3MetaInstitution         string
	S3Size                    int64
	PreservationUrl           string
	StreamSha256              string
	MismatchedMetaInstitution bool
	MismatchedMetaBagName     bool
	MismatchedMetaPath        bool
	MismatchedMetaMd5         bool
	MismatchedMetaSha256      bool
	Error                     string
}

func NewAuditRecord(gfID int64) *AuditRecord {
	return &AuditRecord{
		GenericFileID: gfID,
	}
}

func (ar *AuditRecord) SizeMatches() bool {
	return ar.RegistrySize == ar.S3Size
}

func (ar *AuditRecord) CanCompareEtag() bool {
	return len(ar.S3Etag) > 30 && len(ar.RegistryMd5) > 30 && !strings.Contains(ar.S3Etag, "-")
}

func (ar *AuditRecord) EtagMatches() bool {
	return ar.S3Etag == ar.RegistryMd5
}

func (ar *AuditRecord) HasMetadataMismatch() bool {
	return ar.MismatchedMetaBagName || ar.MismatchedMetaInstitution || ar.MismatchedMetaMd5 || ar.MismatchedMetaPath || ar.MismatchedMetaSha256
}

func (ar *AuditRecord) NeedsFixityCheck() bool {
	if !ar.SizeMatches() {
		return true
	}
	return ar.CanCompareEtag() && !ar.EtagMatches()
}

var CsvHeaders = []string{
	"GenericFileID",
	"CheckPassed",
	"Method",
	"ReasonForCheck",
	"RegistrySize",
	"S3Size",
	"IsGlacierOnlyFile",
	"NeedsGlacierFixityCheck",
	"S3Etag",
	"RegistryMd5",
	"S3MetaMd5",
	"RegistrySha256",
	"S3MetaSha256",
	"StreamSha256",
	"MismatchedMetaInstitution",
	"MismatchedMetaBagName",
	"MismatchedMetaPath",
	"MismatchedMetaMd5",
	"MismatchedMetaSha256",
	"GenericFileCreatedAt",
	"GenericFileUpdatedAt",
	"S3MetaPathInBag",
	"S3MetaBagName",
	"S3MetaInstitution",
	"PreservationUrl",
	"CheckStartedAt",
	"CheckCompletedAt",
	"GenericFileIdentifier",
	"Error",
}

func (ar *AuditRecord) CsvValues() []string {
	return []string{
		strconv.FormatInt(ar.GenericFileID, 10),
		strconv.FormatBool(ar.CheckPassed),
		ar.Method,
		ar.ReasonForCheck,
		strconv.FormatInt(ar.RegistrySize, 10),
		strconv.FormatInt(ar.S3Size, 10),
		strconv.FormatBool(ar.IsGlacierOnlyFile),
		strconv.FormatBool(ar.NeedsGlacierFixityCheck),
		ar.S3Etag,
		ar.RegistryMd5,
		ar.S3MetaMd5,
		ar.RegistrySha256,
		ar.S3MetaSha256,
		ar.StreamSha256,
		strconv.FormatBool(ar.MismatchedMetaInstitution),
		strconv.FormatBool(ar.MismatchedMetaBagName),
		strconv.FormatBool(ar.MismatchedMetaPath),
		strconv.FormatBool(ar.MismatchedMetaMd5),
		strconv.FormatBool(ar.MismatchedMetaSha256),
		ar.GenericFileCreatedAt.Format(time.RFC3339),
		ar.GenericFileUpdatedAt.Format(time.RFC3339),
		ar.S3MetaPathInBag,
		ar.S3MetaBagName,
		ar.S3MetaInstitution,
		ar.PreservationUrl,
		ar.CheckStartedAt.Format(time.RFC3339),
		ar.CheckCompletedAt.Format(time.RFC3339),
		ar.GenericFileIdentifier,
		ar.Error,
	}
}
