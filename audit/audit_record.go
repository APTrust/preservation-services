package audit

import (
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
	StreamMd5                 string
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
