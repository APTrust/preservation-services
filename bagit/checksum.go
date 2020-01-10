package bagit

type Checksum struct {
	Algorithm string `json:"algorithm"`
	Digest    string `json:"digest"`
	Path      string `json:"path"`
}
