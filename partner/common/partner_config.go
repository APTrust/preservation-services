package common

import ()

type PartnerConfig struct {
	APTrustAPIUser string
	APTrustAPIKey  string
}

func LoadPartnerConfig(configFile string) (*PartnerConfig, error) {
	// TODO: Use viper to load config file.
	// If configFile is empty string, try default config file
	return nil, nil
}

func DefaultConfigFileExists() bool {
	// TODO: Implement
	return false
}
