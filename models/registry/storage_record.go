package registry

type StorageRecord struct {
	GenericFileID int    `json:"generic_file_id"`
	ID            int    `json:"id,omitempty"`
	URL           string `json:"url"`
}
