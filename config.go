package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ConfigFile struct {
	API       string          `json:"api"`
	APIUser   string          `json:"api_user"`
	APIPass   string          `json:"api_pass"`
	Frontends []FrontendEntry `json:"frontends"`
}

type FrontendEntry struct {
	URL      string   `json:"url"`
	Backends []string `json:"backends"`
}

func loadConfigFile(path string) (*ConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cf ConfigFile
	cf.API = ":9090"
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	if len(cf.Frontends) == 0 {
		return nil, fmt.Errorf("config file has no frontends")
	}
	return &cf, nil
}

func configToProxyConfigs(cf *ConfigFile) ([]FrontendConfig, error) {
	configs := make([]FrontendConfig, 0, len(cf.Frontends))
	for i, fe := range cf.Frontends {
		fcfg, err := parseFrontendURL(fe.URL)
		if err != nil {
			return nil, fmt.Errorf("frontend %d: %w", i, err)
		}
		backends, err := parseBackends(strings.Join(fe.Backends, ","))
		if err != nil {
			return nil, fmt.Errorf("frontend %d backends: %w", i, err)
		}
		fcfg.Backends = backends
		configs = append(configs, fcfg)
	}
	return configs, nil
}
