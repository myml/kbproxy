package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
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
	} else {
		mux.HandleFunc("/", p.serveIndex)
		mux.HandleFunc("/api/connections", p.handleConnections)
		mux.HandleFunc("/api/stats", p.handleStats)
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
	ID         string `json:"id"`
	ListenAddr string `json:"listen_addr"`
	TotalConns int64  `json:"total_conns"`
	PeakConns  int64  `json:"peak_conns"`
	PeakRateIn  int64  `json:"peak_rate_in"`
	PeakRateOut int64  `json:"peak_rate_out"`
}

type backendStat struct {
	Addr        string `json:"addr"`
	Weight      int    `json:"weight"`
	TotalConns  int64  `json:"total_conns"`
	PeakConns   int64  `json:"peak_conns"`
	PeakRateIn  int64  `json:"peak_rate_in"`
	PeakRateOut int64  `json:"peak_rate_out"`
	Healthy     bool   `json:"healthy"`
}

func (p *Proxy) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	p.stats.mu.Lock()
	frontends := make([]frontendStat, 0, len(p.stats.frontends))
	backends := make([]backendStat, 0)
	for _, fs := range p.stats.frontends {
		frontends = append(frontends, frontendStat{
			ID:          fs.id,
			ListenAddr:  fs.listenAddr,
			TotalConns:  fs.totalConns.Load(),
			PeakConns:   fs.peakConns.Load(),
			PeakRateIn:  fs.peakRateIn.Load(),
			PeakRateOut: fs.peakRateOut.Load(),
		})
		for _, bs := range fs.backends {
			backends = append(backends, backendStat{
				Addr:        bs.addr,
				Weight:      bs.weight,
				TotalConns:  bs.totalConns.Load(),
				PeakConns:   bs.peakConns.Load(),
				PeakRateIn:  bs.peakRateIn.Load(),
				PeakRateOut: bs.peakRateOut.Load(),
				Healthy:     bs.healthy.Load(),
			})
		}
	}
	p.stats.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]any{
		"frontends": frontends,
		"backends":  backends,
	})
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
