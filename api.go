package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"time"
)

//go:embed index.html
var indexHTML []byte

func (p *Proxy) startAPI() error {
	mux := http.NewServeMux()

	if p.apiUser != "" {
		mux.HandleFunc("/", p.basicAuth(p.serveIndex))
		mux.HandleFunc("/api/connections", p.basicAuth(p.handleConnections))
		mux.HandleFunc("/api/stats", p.basicAuth(p.handleStats))
		mux.HandleFunc("/api/test", p.basicAuth(p.handleTest))
		mux.HandleFunc("/api/reload", p.basicAuth(p.handleReload))
	} else {
		mux.HandleFunc("/", p.serveIndex)
		mux.HandleFunc("/api/connections", p.handleConnections)
		mux.HandleFunc("/api/stats", p.handleStats)
		mux.HandleFunc("/api/test", p.handleTest)
		mux.HandleFunc("/api/reload", p.handleReload)
	}

	if addr := os.Getenv("PPROF_ADDR"); addr != "" {
		go http.ListenAndServe(addr, nil)
	}
	return http.ListenAndServe(p.apiAddr, mux)
}

func (p *Proxy) basicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != p.apiUser || pass != p.apiPass {
			w.Header().Set("WWW-Authenticate", `Basic realm="kbproxy"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (p *Proxy) serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

type connInfo struct {
	ID        string  `json:"id"`
	Frontend  string  `json:"frontend"`
	Backend   string  `json:"backend"`
	BytesIn   int64   `json:"bytes_in"`
	BytesOut  int64   `json:"bytes_out"`
	RateIn    float64 `json:"rate_in"`
	RateOut   float64 `json:"rate_out"`
	CreatedAt string  `json:"created_at"`
}

func (p *Proxy) handleConnections(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	p.stats.mu.Lock()
	conns := make([]connInfo, 0, len(p.stats.conns))
	for _, cs := range p.stats.conns {
		dur := time.Since(cs.createdAt)
		conns = append(conns, connInfo{
			ID:        cs.id,
			Frontend:  cs.fs.listenAddr,
			Backend:   cs.bs.addr,
			BytesIn:   cs.bytesIn.Load(),
			BytesOut:  cs.bytesOut.Load(),
			RateIn:    cs.rateIn.rate(),
			RateOut:   cs.rateOut.rate(),
			CreatedAt: fmtDuration(dur),
		})
	}
	p.stats.mu.Unlock()

	json.NewEncoder(w).Encode(conns)
}

type frontendStat struct {
	ID          string        `json:"id"`
	ListenAddr  string        `json:"listen_addr"`
	RateLimit   int64         `json:"rate_limit"`
	TotalConns  int64         `json:"total_conns"`
	PeakConns   int64         `json:"peak_conns"`
	PeakRateIn  int64         `json:"peak_rate_in"`
	PeakRateOut int64         `json:"peak_rate_out"`
	Backends    []backendStat `json:"backends"`
}

type backendStat struct {
	Addr           string `json:"addr"`
	Weight         int    `json:"weight"`
	Backup         bool   `json:"backup"`
	TotalConns     int64  `json:"total_conns"`
	PeakConns      int64  `json:"peak_conns"`
	PeakRateIn     int64  `json:"peak_rate_in"`
	PeakRateOut    int64  `json:"peak_rate_out"`
	Healthy        bool   `json:"healthy"`
	LastCheckTime  int64  `json:"last_check_time"`
	CheckInterval  int64  `json:"check_interval"`
	CheckTotal     int64  `json:"check_total"`
	CheckSuccess   int64  `json:"check_success"`
}

func (p *Proxy) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	p.stats.mu.Lock()
	frontends := make([]frontendStat, 0, len(p.stats.frontends))
	for _, fs := range p.stats.frontends {
		var beList []backendStat
		for _, bs := range fs.backends {
		beList = append(beList, backendStat{
			Addr:          bs.addr,
			Weight:        bs.weight,
			Backup:        bs.backup,
				TotalConns:    bs.totalConns.Load(),
				PeakConns:     bs.peakConns.Load(),
				PeakRateIn:    bs.peakRateIn.Load(),
				PeakRateOut:   bs.peakRateOut.Load(),
				Healthy:       bs.healthy.Load(),
				LastCheckTime: bs.lastCheckTime.Load(),
				CheckInterval: bs.checkInterval.Load(),
				CheckTotal:    bs.checkTotal.Load(),
				CheckSuccess:  bs.checkSuccess.Load(),
			})
		}
		frontends = append(frontends, frontendStat{
			ID:          fs.id,
			ListenAddr:  fs.listenAddr,
			RateLimit:   fs.rateLimit,
			TotalConns:  fs.totalConns.Load(),
			PeakConns:   fs.peakConns.Load(),
			PeakRateIn:  fs.peakRateIn.Load(),
			PeakRateOut: fs.peakRateOut.Load(),
			Backends:    beList,
		})
	}
	sort.Slice(frontends, func(i, j int) bool {
		return frontends[i].ID < frontends[j].ID
	})
	p.stats.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]any{
		"frontends": frontends,
	})
}

func (p *Proxy) handleTest(w http.ResponseWriter, r *http.Request) {
	size := int64(100 * 1024 * 1024)
	if v := r.URL.Query().Get("size"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			size = n
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("Content-Disposition", "attachment; filename=test")
	buf := make([]byte, 32*1024)
	for i := range buf {
		buf[i] = byte(i % 256)
	}
	var written int64
	for written < size {
		remain := size - written
		if int64(len(buf)) > remain {
			w.Write(buf[:remain])
			written = size
		} else {
			w.Write(buf)
			written += int64(len(buf))
		}
	}
}

func (p *Proxy) handleReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if p.configPath == "" {
		http.Error(w, "Hot reload not enabled (use -config)", http.StatusBadRequest)
		return
	}

	cf, err := loadConfigFile(p.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	configs, err := configToProxyConfigs(cf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if cf.API != p.apiAddr {
		fmt.Printf("[reload] WARNING: api address change requires restart, ignoring\n")
	}
	p.apiUser = cf.APIUser
	p.apiPass = cf.APIPass
	p.ReloadConfig(configs)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
