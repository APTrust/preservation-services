package registry

type StorageRecord struct {
	GenericFileID int    `json:"generic_file_id"`
	ID            int    `json:"id"`
	URL           string `json:"url"`
}
