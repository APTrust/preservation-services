package bagit

// Tag describes a tag parsed from a BagIt file such as bag-info.txt.
type Tag struct {
	TagFile string `json:"tag_file"`
	TagName string `json:"tag_name"`
	Value   string `json:"value"`
}

func NewTag(sourceFile, label, value string) *Tag {
	return &Tag{
		TagFile: sourceFile,
		TagName: label,
		Value:   value,
	}
}
