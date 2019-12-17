package bagit

// MissingFile defines a file that is not in the bag, despite the
// fact that its checksum was found in a manifest. We keep track
// of these during bag validation, so we can report which files
// were not found.
type MissingFile struct {
	Manifest   string `json:"manifest"`
	LineNumber int    `json:"line_number"`
	FilePath   string `json:"file_path"`
	Digest     string `json:"digest"`
}

func NewMissingFile(manifest string, lineNumber int, filePath, digest string) *MissingFile {
	return &MissingFile{
		Manifest:   manifest,
		LineNumber: lineNumber,
		FilePath:   filePath,
		Digest:     digest,
	}
}
