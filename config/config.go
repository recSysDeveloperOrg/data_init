package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Mongo *MongoDB `json:"mongodb"`
}

type MongoDB struct {
	Url      string `json:"url"`
	DBName   string `json:"DBName"`
	User     string `json:"user"`
	Password string `json:"password"`
}

var cfg Config
var cfgFileName = "config/prod_conf.json"

func GetConfig() *Config {
	return &cfg
}

func InitConfig() error {
	content, err := ioutil.ReadFile(cfgFileName)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(content, &cfg); err != nil {
		return err
	}

	return nil
}
