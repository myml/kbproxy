package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type BackendConfig struct {
	Addr          string
	Weight        int
	Backup        bool
	CheckScript   string
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}

type FrontendConfig struct {
	ListenAddr string
	Backends   []BackendConfig
	LBStrategy string
}

func parseFrontendURL(raw string) (FrontendConfig, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return FrontendConfig{}, fmt.Errorf("invalid frontend URL %q: %w", raw, err)
	}
	host := u.Host
	if host == "" {
		host = u.Path
	}
	lb := u.Query().Get("lb")
	if lb == "" {
		lb = "least_bandwidth"
	}
	return FrontendConfig{ListenAddr: host, LBStrategy: lb}, nil
}

func parseBackendURL(raw string) (BackendConfig, error) {
	if !strings.HasPrefix(raw, "tcp://") {
		return BackendConfig{}, fmt.Errorf("invalid backend URL %q: must start with tcp://", raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return BackendConfig{}, fmt.Errorf("invalid backend URL %q: %w", raw, err)
	}
	if u.Host == "" {
		return BackendConfig{}, fmt.Errorf("invalid backend URL %q: missing host", raw)
	}
	weight := 1
	if w := u.Query().Get("weight"); w != "" {
		weight, err = strconv.Atoi(w)
		if err != nil || weight <= 0 {
			return BackendConfig{}, fmt.Errorf("invalid backend URL %q: weight must be a positive integer", raw)
		}
	}
	checkScript := u.Query().Get("check")

	backup := false
	if u.Query().Has("backup") {
		if v := u.Query().Get("backup"); v != "" {
			backup, _ = strconv.ParseBool(v)
		} else {
			backup = true
		}
	}

	checkInterval := 60 * time.Second
	if v := u.Query().Get("inter"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs <= 0 {
			return BackendConfig{}, fmt.Errorf("invalid backend URL %q: inter must be a positive integer", raw)
		}
		checkInterval = time.Duration(secs) * time.Second
	}

	checkTimeout := 5 * time.Second
	if v := u.Query().Get("check_timeout"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs <= 0 {
			return BackendConfig{}, fmt.Errorf("invalid backend URL %q: check_timeout must be a positive integer", raw)
		}
		checkTimeout = time.Duration(secs) * time.Second
	}

	return BackendConfig{
		Addr:          u.Host,
		Weight:        weight,
		Backup:        backup,
		CheckScript:   checkScript,
		CheckInterval: checkInterval,
		CheckTimeout:  checkTimeout,
	}, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: kbproxy [flags]\n\nFlags:\n")
		flag.PrintDefaults()
	}

	var apiAddr string
	var apiUser string
	var apiPass string

	flag.StringVar(&apiAddr, "api", ":9090", "API server listen address")
	flag.StringVar(&apiUser, "api-user", "", "API Basic Auth username")
	flag.StringVar(&apiPass, "api-pass", "", "API Basic Auth password")

	var frontendURLs multiFlag
	var backendLists multiFlag

	flag.Var(&frontendURLs, "frontend", "Frontend URL: tcp://:8080?lb=least_conn (repeatable)")
	flag.Var(&backendLists, "backend", "Comma-separated backend URLs: tcp://10.0.0.1:80,tcp://10.0.0.2:80")

	flag.Parse()

	if len(frontendURLs) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	configs := make([]FrontendConfig, len(frontendURLs))
	for i, raw := range frontendURLs {
		cfg, err := parseFrontendURL(raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if i < len(backendLists) {
			backends, err := parseBackends(backendLists[i])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			cfg.Backends = backends
		}
		configs[i] = cfg
	}

	proxy := NewProxy(configs, apiAddr, apiUser, apiPass)
	if err := proxy.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprintf("%v", *m) }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func parseBackends(s string) ([]BackendConfig, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	backends := make([]BackendConfig, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		bc, err := parseBackendURL(p)
		if err != nil {
			return nil, err
		}
		backends = append(backends, bc)
	}
	return backends, nil
}
