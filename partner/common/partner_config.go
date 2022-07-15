package common

import (
	"fmt"
	"os"
	"path"

	"github.com/spf13/viper"
)

type PartnerConfig struct {
	APTrustAPIUser string
	APTrustAPIKey  string
}

func LoadPartnerConfig(configFile string) (*PartnerConfig, error) {
	var err error
	var dir string
	filename := ".aptrust_partner.conf"
	if configFile == "" {
		dir, err = os.UserHomeDir()
		if err != nil {
			return nil, err
		}
	} else {
		dir = path.Dir(configFile)
		filename = path.Base(configFile)
	}
	v := viper.New()
	v.AddConfigPath(dir)
	v.SetConfigName(filename)
	v.SetConfigType("env")
	v.AutomaticEnv() // so env vars override file vars
	err = v.ReadInConfig()
	if err != nil {
		return nil, err
	}
	config := &PartnerConfig{
		APTrustAPIUser: v.GetString("APTRUST_API_USER"),
		APTrustAPIKey:  v.GetString("APTRUST_API_KEY"),
	}
	return config, err
}

func LoadFromEnv() (*PartnerConfig, error) {
	var conf *PartnerConfig
	var err error
	user := os.Getenv("APTRUST_API_USER")
	key := os.Getenv("APTRUST_API_KEY")
	if len(user) > 0 && len(key) > 0 {
		conf = &PartnerConfig{
			APTrustAPIUser: user,
			APTrustAPIKey:  key,
		}
	} else {
		err = fmt.Errorf("config vars are not set in environment")
	}
	return conf, err
}
