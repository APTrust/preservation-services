package bagit

// TODO: Change SourceFile to TagFile and Label to TagName
// to bring this in line with TagDef in both preservation-services
// and DART.

// Tag describes a tag parsed from a BagIt file such as bag-info.txt.
type Tag struct {
	SourceFile string `json:"source_file"`
	Label      string `json:"label"`
	Value      string `json:"value"`
}

func NewTag(sourceFile, label, value string) *Tag {
	return &Tag{
		SourceFile: sourceFile,
		Label:      label,
		Value:      value,
	}
}
