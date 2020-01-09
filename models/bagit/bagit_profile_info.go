package bagit

type BagItProfileInfo struct {
	BagItProfileIdentifier string `json:"bagItProfileIdentifier"`
	BagItProfileVersion    string `json:"bagItProfileVersion"`
	ContactEmail           string `json:"contactEmail"`
	ContactName            string `json:"contactName"`
	ExternalDescription    string `json:"externalDescription"`
	SourceOrganization     string `json:"sourceOrganization"`
}
