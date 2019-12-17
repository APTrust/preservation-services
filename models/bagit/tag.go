package bagit

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
