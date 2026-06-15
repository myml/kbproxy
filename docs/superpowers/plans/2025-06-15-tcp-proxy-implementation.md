# kbproxy TCP Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a TCP proxy in Go that forwards a single port to multiple backend servers with three-layer traffic monitoring and a real-time Web UI.

**Architecture:** Single binary with no external dependencies. TCP listener accepts connections, selects a backend via load balancer, proxies data bidirectionally with byte-counting. Stats collector maintains three layers (frontend/backend/connection) with sliding-window rate calculation. HTTP server serves REST API and embedded HTML frontend.

**Tech Stack:** Go 1.21+, standard library only (net, net/http, embed, sync, encoding/json)

---

### Task 1: Project scaffold + go.mod

**Files:**
- Create: `go.mod`
- Create: `main.go` (stub that prints usage)

- [ ] **Step 1: Initialize go module**

Run: `go mod init kbproxy`

- [ ] **Step 2: Write main.go stub**

```go
package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
)

type BackendConfig struct {
	Addr string
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
		lb = "least_conn"
	}
	return FrontendConfig{ListenAddr: host, LBStrategy: lb}, nil
}

func parseBackendURL(raw string) (string, error) {
	if !strings.HasPrefix(raw, "tcp://") {
		return "", fmt.Errorf("invalid backend URL %q: must start with tcp://", raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid backend URL %q: %w", raw, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("invalid backend URL %q: missing host", raw)
	}
	return u.Host, nil
}

func main() {
	var apiAddr string
	var apiUser string
	var apiPass string

	flag.StringVar(&apiAddr, "api", ":9090", "API server listen address")
	flag.StringVar(&apiUser, "api-user", "", "API Basic Auth username")
	flag.StringVar(&apiPass, "api-pass", "", "API Basic Auth password")

	var frontendURLs multiFlag
	var backendLists multiFlag

	flag.Var(&frontendURLs, "frontend", "Frontend URL: tcp://:8080?lb=least_conn (can be specified multiple times)")
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
		addr, err := parseBackendURL(p)
		if err != nil {
			return nil, err
		}
		backends = append(backends, BackendConfig{Addr: addr})
	}
	return backends, nil
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./...`
Expected: binary `kbproxy` built with no errors.

- [ ] **Step 4: Verify usage output**

Run: `./kbproxy`
Expected: prints usage message and exits with code 1.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: project scaffold and CLI flag parsing"
```

---

### Task 2: Stats collector - connection-level tracking

**Files:**
- Create: `stats.go`

This task implements the core data structures for the three-layer statistics, plus the sliding-window rate calculator.

- [ ] **Step 1: Create stats.go with data types and sliding window**

```go
package main

import (
	"sync"
	"sync/atomic"
	"time"
)

const windowSize = 10

type slidingWindow struct {
	mu   sync.Mutex
	buf  [windowSize]int64
	pos  int
	full bool
}

func newSlidingWindow() *slidingWindow {
	return &slidingWindow{}
}

func (sw *slidingWindow) add(sample int64) {
	sw.mu.Lock()
	sw.buf[sw.pos] = sample
	sw.pos++
	if sw.pos >= windowSize {
		sw.pos = 0
		sw.full = true
	}
	sw.mu.Unlock()
}

func (sw *slidingWindow) rate() float64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	var total int64
	count := sw.pos
	if sw.full {
		count = windowSize
	}
	for i := 0; i < count; i++ {
		total += sw.buf[i]
	}
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

type connStats struct {
	id              string
	bytesIn         atomic.Int64
	bytesOut        atomic.Int64
	samplingBytesIn  atomic.Int64
	samplingBytesOut atomic.Int64
	rateIn          *slidingWindow
	rateOut         *slidingWindow
	createdAt       time.Time
	closed          atomic.Bool
}

func newConnStats(id string) *connStats {
	return &connStats{
		id:        id,
		rateIn:    newSlidingWindow(),
		rateOut:   newSlidingWindow(),
		createdAt: time.Now(),
	}
}

type backendStats struct {
	addr             string
	bytesIn          atomic.Int64
	bytesOut         atomic.Int64
	rateIn           *slidingWindow
	rateOut          *slidingWindow
	samplingBytesIn  atomic.Int64
	samplingBytesOut atomic.Int64
	activeConns      atomic.Int64
}

func newBackendStats(addr string) *backendStats {
	return &backendStats{
		addr:   addr,
		rateIn:  newSlidingWindow(),
		rateOut: newSlidingWindow(),
	}
}

type frontendStats struct {
	id               string
	listenAddr       string
	bytesIn          atomic.Int64
	bytesOut         atomic.Int64
	rateIn           *slidingWindow
	rateOut          *slidingWindow
	samplingBytesIn  atomic.Int64
	samplingBytesOut atomic.Int64
	activeConns      atomic.Int64
	backends         []*backendStats
}

func newFrontendStats(id, listenAddr string) *frontendStats {
	return &frontendStats{
		id:        id,
		listenAddr: listenAddr,
		rateIn:    newSlidingWindow(),
		rateOut:   newSlidingWindow(),
	}
}
```

- [ ] **Step 2: Create stats collector with periodic sampling**

```go
type statsCollector struct {
	mu        sync.Mutex
	frontends map[string]*frontendStats
	conns     map[string]*connStats
}

func newStatsCollector() *statsCollector {
	return &statsCollector{
		frontends: make(map[string]*frontendStats),
		conns:     make(map[string]*connStats),
	}
}

func (sc *statsCollector) registerFrontend(id, listenAddr string, backendAddrs []string) *frontendStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	fs := newFrontendStats(id, listenAddr)
	for _, addr := range backendAddrs {
		fs.backends = append(fs.backends, newBackendStats(addr))
	}
	sc.frontends[id] = fs
	return fs
}

func (sc *statsCollector) registerConn(id string, fs *frontendStats, bs *backendStats) *connStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cs := newConnStats(id)
	cs.bs = bs
	cs.fs = fs
	sc.conns[id] = cs
	fs.activeConns.Add(1)
	bs.activeConns.Add(1)
	return cs
}

func (sc *statsCollector) unregisterConn(id string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cs, ok := sc.conns[id]
	if !ok {
		return
	}
	cs.closed.Store(true)
	cs.fs.activeConns.Add(-1)
	cs.bs.activeConns.Add(-1)
	delete(sc.conns, id)
}

func (sc *statsCollector) startSampling() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			sc.sampleAll()
		}
	}()
}

func (sc *statsCollector) sampleAll() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for _, cs := range sc.conns {
		if cs.closed.Load() {
			continue
		}
		in := cs.samplingBytesIn.Swap(0)
		out := cs.samplingBytesOut.Swap(0)
		cs.rateIn.add(in)
		cs.rateOut.add(out)
	}

	for _, fs := range sc.frontends {
		var totalIn, totalOut int64
		for _, bs := range fs.backends {
			bin := bs.samplingBytesIn.Swap(0)
			bout := bs.samplingBytesOut.Swap(0)
			bs.rateIn.add(bin)
			bs.rateOut.add(bout)
			totalIn += bin
			totalOut += bout
		}
		fs.samplingBytesIn.Add(totalIn)
		fs.samplingBytesOut.Add(totalOut)
		fin := fs.samplingBytesIn.Swap(0)
		fout := fs.samplingBytesOut.Swap(0)
		fs.rateIn.add(fin)
		fs.rateOut.add(fout)
	}
}
```

We need to add the `bs` and `fs` fields to `connStats`:

- [ ] **Step 3: Update connStats struct**

Edit `stats.go`: add `bs *backendStats` and `fs *frontendStats` fields:

```go
type connStats struct {
	id               string
	bytesIn          atomic.Int64
	bytesOut         atomic.Int64
	samplingBytesIn   atomic.Int64
	samplingBytesOut  atomic.Int64
	rateIn           *slidingWindow
	rateOut          *slidingWindow
	createdAt        time.Time
	closed           atomic.Bool
	bs               *backendStats   
	fs               *frontendStats  
}
```

- [ ] **Step 4: Verify compilation**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: stats collector with sliding window rate calculation"
```

---

### Task 3: Load balancer

**Files:**
- Create: `lb.go`

- [ ] **Step 1: Create lb.go with both strategies**

```go
package main

import (
	"math"
)

type loadBalancer interface {
	Pick(backends []*backendStats) *backendStats
}

type leastConnectionsLB struct{}

func (leastConnectionsLB) Pick(backends []*backendStats) *backendStats {
	if len(backends) == 0 {
		return nil
	}
	selected := backends[0]
	min := selected.activeConns.Load()
	for _, b := range backends[1:] {
		if n := b.activeConns.Load(); n < min {
			min = n
			selected = b
		}
	}
	return selected
}

type leastBandwidthLB struct{}

func (leastBandwidthLB) Pick(backends []*backendStats) *backendStats {
	if len(backends) == 0 {
		return nil
	}
	selected := backends[0]
	min := selected.rateOut.rate()
	for _, b := range backends[1:] {
		if r := b.rateOut.rate(); r < min {
			min = r
			selected = b
		}
	}
	return selected
}

func newLoadBalancer(strategy string) loadBalancer {
	switch strategy {
	case "least_bandwidth":
		return leastBandwidthLB{}
	case "least_conn":
		fallthrough
	default:
		return leastConnectionsLB{}
	}
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: least-connections and least-bandwidth load balancer"
```

---

### Task 4: Proxy core - TCP listener and connection forwarding

**Files:**
- Create: `proxy.go`

- [ ] **Step 1: Create proxy.go with Proxy struct and Start method**

```go
package main

import (
	"fmt"
	"io"
	"net"
	"time"
)

type Proxy struct {
	configs  []FrontendConfig
	apiAddr  string
	apiUser  string
	apiPass  string
	stats    *statsCollector
	lbCache  map[string]loadBalancer
	connID   int64
}

func NewProxy(configs []FrontendConfig, apiAddr, apiUser, apiPass string) *Proxy {
	return &Proxy{
		configs: configs,
		apiAddr: apiAddr,
		apiUser: apiUser,
		apiPass: apiPass,
		stats:   newStatsCollector(),
		lbCache: make(map[string]loadBalancer),
	}
}

func (p *Proxy) Start() error {
	p.stats.startSampling()

	for i, cfg := range p.configs {
		id := fmt.Sprintf("f-%d", i)
		backendAddrs := make([]string, len(cfg.Backends))
		for j, b := range cfg.Backends {
			backendAddrs[j] = b.Addr
		}
		fs := p.stats.registerFrontend(id, cfg.ListenAddr, backendAddrs)
		p.lbCache[id] = newLoadBalancer(cfg.LBStrategy)

		go p.listenFrontend(cfg, fs)
	}

	return p.startAPI()
}

func (p *Proxy) listenFrontend(cfg FrontendConfig, fs *frontendStats) error {
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	defer listener.Close()

	fmt.Printf("[proxy] listening on %s (frontend: %s)\n", cfg.ListenAddr, fs.id)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Printf("[proxy] accept error on %s: %v\n", cfg.ListenAddr, err)
			continue
		}
		go p.handleConnection(clientConn, cfg, fs)
	}
}

func (p *Proxy) handleConnection(clientConn net.Conn, cfg FrontendConfig, fs *frontendStats) {
	defer clientConn.Close()

	lb := p.lbCache[fs.id]
	bs := lb.Pick(fs.backends)
	if bs == nil {
		fmt.Printf("[proxy] no backends available for %s\n", fs.id)
		return
	}

	backendConn, err := net.DialTimeout("tcp", bs.addr, 10*time.Second)
	if err != nil {
		fmt.Printf("[proxy] connect to backend %s: %v\n", bs.addr, err)
		return
	}
	defer backendConn.Close()

	p.connID++
	connID := fmt.Sprintf("c-%d", p.connID)
	cs := p.stats.registerConn(connID, fs, bs)

	fmt.Printf("[proxy] %s: %s <-> %s via %s\n", connID, clientConn.RemoteAddr(), bs.addr, fs.listenAddr)

	done := make(chan bool, 2)
	// client → backend: data goes OUT from proxy to backend
	go p.pipe(clientConn, backendConn, cs, &cs.bytesOut, &cs.samplingBytesOut, &cs.bs.bytesIn, &cs.fs.bytesIn, done)
	// backend → client: data comes IN to proxy from backend
	go p.pipe(backendConn, clientConn, cs, &cs.bytesIn, &cs.samplingBytesIn, &cs.bs.bytesOut, &cs.fs.bytesOut, done)

	<-done
	<-done

	cs.closed.Store(true)
	p.stats.unregisterConn(connID)
	fmt.Printf("[proxy] %s: closed\n", connID)
}

func (p *Proxy) pipe(dst io.Writer, src io.Reader, cs *connStats, total, sampling *atomic.Int64, done chan bool) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			cs.bs.bytesOut.Add(int64(n))
			cs.fs.bytesOut.Add(int64(n))
			total.Add(int64(n))
			sampling.Add(int64(n))
			wn, werr := dst.Write(buf[:n])
			if wn < n {
				done <- true
				return
			}
			if werr != nil {
				done <- true
				return
			}
		}
		if err != nil {
			done <- true
			return
		}
	}
}
```

Note: the `pipe` function uses `atomic.Int64` — need to import it. Also the `cs.bs` and `cs.fs` references need to guard against nil. Let me refine:

Actually, I need to re-check the atomic usage. `cs.bs.bytesOut` — we should increment backend stats from within `connStats` tracking. Let me simplify: the pipe function only updates the connection-level atomics, and the backend/frontend stats get updated within handleConnection.

Wait, let me rethink. The `pipe` function should track bytes for the connection. The backend/frontend stats are derived from connection stats via the sampling mechanism. Actually in my stats.go design, the sampling collects data from connection-level samplingBytesIn/Out, then aggregates to backend and frontend. So the pipe only needs to update connection-level stats.

Let me fix the pipe to only update connection-level atomics:

```go
func (p *Proxy) pipe(dst io.Writer, src io.Reader, cs *connStats, connTotal, connSampling, backendTotal, frontendTotal *atomic.Int64, done chan bool) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			connTotal.Add(int64(n))
			connSampling.Add(int64(n))
			backendTotal.Add(int64(n))
			frontendTotal.Add(int64(n))
			wn, werr := dst.Write(buf[:n])
			if werr != nil {
				done <- true
				return
			}
			_ = wn
		}
		if err != nil {
			done <- true
			return
		}
	}
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: TCP proxy core with connection handling and bidirectional pipe"
```

---

### Task 5: API server

**Files:**
- Create: `api.go`

- [ ] **Step 1: Create api.go with REST endpoints**

```go
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
)

func (p *Proxy) startAPI() error {
	mux := http.NewServeMux()

	if p.apiUser != "" {
		mux.HandleFunc("/", p.basicAuth(p.handleRoot))
		mux.HandleFunc("/api/", p.basicAuth(p.handleAPI))
	} else {
		mux.HandleFunc("/", p.handleRoot)
		mux.HandleFunc("/api/", p.handleAPI)
	}

	fmt.Printf("[api] listening on %s\n", p.apiAddr)
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

func (p *Proxy) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (p *Proxy) handleAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.URL.Path {
	case "/api/frontends":
		p.handleFrontends(w, r)
	case "/api/backends":
		p.handleBackends(w, r)
	default:
		// /api/frontends/:id/connections
		var frontendID string
		if n, _ := fmt.Sscanf(r.URL.Path, "/api/frontends/%s/connections", &frontendID); n == 1 {
			p.handleConnections(w, r, frontendID)
		} else {
			http.NotFound(w, r)
		}
	}
}

type frontendAPI struct {
	ID               string         `json:"id"`
	ListenAddr       string         `json:"listen_addr"`
	ActiveConnections int64         `json:"active_connections"`
	BytesIn          int64          `json:"bytes_in"`
	BytesOut         int64          `json:"bytes_out"`
	RateIn           float64        `json:"rate_in"`
	RateOut          float64        `json:"rate_out"`
	Backends         []backendAPI   `json:"backends"`
}

type backendAPI struct {
	Addr              string  `json:"addr"`
	ActiveConnections int64   `json:"active_connections"`
	BytesIn           int64   `json:"bytes_in"`
	BytesOut          int64   `json:"bytes_out"`
	RateIn            float64 `json:"rate_in"`
	RateOut           float64 `json:"rate_out"`
}

type connAPI struct {
	ID        string  `json:"id"`
	Backend   string  `json:"backend"`
	BytesIn   int64   `json:"bytes_in"`
	BytesOut  int64   `json:"bytes_out"`
	RateIn    float64 `json:"rate_in"`
	RateOut   float64 `json:"rate_out"`
	CreatedAt string  `json:"created_at"`
	Closed    bool    `json:"closed"`
}

func (p *Proxy) handleFrontends(w http.ResponseWriter, r *http.Request) {
	p.stats.mu.Lock()
	frontends := make([]frontendAPI, 0, len(p.stats.frontends))
	for _, fs := range p.stats.frontends {
		f := frontendAPI{
			ID:               fs.id,
			ListenAddr:       fs.listenAddr,
			ActiveConnections: fs.activeConns.Load(),
			BytesIn:          fs.bytesIn.Load(),
			BytesOut:         fs.bytesOut.Load(),
			RateIn:           fs.rateIn.rate(),
			RateOut:          fs.rateOut.rate(),
		}
		for _, bs := range fs.backends {
			f.Backends = append(f.Backends, backendAPI{
				Addr:              bs.addr,
				ActiveConnections: bs.activeConns.Load(),
				BytesIn:           bs.bytesIn.Load(),
				BytesOut:          bs.bytesOut.Load(),
				RateIn:            bs.rateIn.rate(),
				RateOut:           bs.rateOut.rate(),
			})
		}
		frontends = append(frontends, f)
	}
	p.stats.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]any{"frontends": frontends})
}

func (p *Proxy) handleBackends(w http.ResponseWriter, r *http.Request) {
	p.stats.mu.Lock()
	var backends []backendAPI
	for _, fs := range p.stats.frontends {
		for _, bs := range fs.backends {
			backends = append(backends, backendAPI{
				Addr:              bs.addr,
				ActiveConnections: bs.activeConns.Load(),
				BytesIn:           bs.bytesIn.Load(),
				BytesOut:          bs.bytesOut.Load(),
				RateIn:            bs.rateIn.rate(),
				RateOut:           bs.rateOut.rate(),
			})
		}
	}
	p.stats.mu.Unlock()

	json.NewEncoder(w).Encode(map[string]any{"backends": backends})
}

func (p *Proxy) handleConnections(w http.ResponseWriter, r *http.Request, frontendID string) {
	p.stats.mu.Lock()
	var conns []connAPI
	for _, cs := range p.stats.conns {
		if cs.fs.id == frontendID {
			conns = append(conns, connAPI{
				ID:        cs.id,
				Backend:   cs.bs.addr,
				BytesIn:   cs.bytesIn.Load(),
				BytesOut:  cs.bytesOut.Load(),
				RateIn:    cs.rateIn.rate(),
				RateOut:   cs.rateOut.rate(),
				CreatedAt: cs.createdAt.Format("15:04:05"),
				Closed:    cs.closed.Load(),
			})
		}
	}
	p.stats.mu.Unlock()

	if conns == nil {
		conns = []connAPI{}
	}
	json.NewEncoder(w).Encode(map[string]any{"connections": conns})
}
```

- [ ] **Step 2: Create the embedded HTML template**

```go
//go:embed index.html
var indexHTML []byte
```

Place this at the top of `api.go` (above the `package` line is wrong — it should be inside the file but outside any function, at the package level):

```go
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
)
```

Actually, the `//go:embed` directive must come before the `var` declaration. Let me put it properly:

```go
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
)

//go:embed index.html
var indexHTML []byte
```

- [ ] **Step 3: Verify compilation (will fail — no index.html yet)**

Run: `go build ./...`
Expected: error about index.html not found. That's OK — next task creates it.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: API server with REST endpoints and Basic Auth"
```

---

### Task 6: HTML frontend

**Files:**
- Create: `index.html`

- [ ] **Step 1: Create index.html**

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>kbproxy</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, 'Segoe UI', monospace; background: #0f1923; color: #cdd6f4; padding: 20px; }
h1 { font-size: 18px; margin-bottom: 16px; color: #89b4fa; }
h2 { font-size: 14px; margin: 20px 0 8px; color: #a6e3a1; }
table { width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 20px; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #313244; }
th { color: #6c7086; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
tr:hover td { background: #1e1e2e; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
.bar-wrap { display: inline-block; width: 80px; height: 6px; background: #313244; border-radius: 3px; vertical-align: middle; }
.bar-fill { height: 100%; border-radius: 3px; background: #89b4fa; transition: width 0.5s; }
.bar-fill.out { background: #f9e2af; }
.tag { display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; background: #313244; color: #bac2de; }
.tag.active { background: #a6e3a1; color: #0f1923; }
.refresh-info { font-size: 11px; color: #585b70; margin-bottom: 12px; }
.detail-conns { display: none; }
.detail-conns.show { display: table-row-group; }
.conn-row td { background: #11111b; font-size: 11px; }
.loading { color: #585b70; font-size: 12px; }
</style>
</head>
<body>
<h1>kbproxy &mdash; TCP Proxy Monitor</h1>
<p class="refresh-info" id="refreshInfo">Refreshing every 1s</p>

<div id="content">
<p class="loading">Loading...</p>
</div>

<script>
const API = '/api';

function fmt(n) {
  if (n >= 1073741824) return (n / 1073741824).toFixed(1) + ' GiB';
  if (n >= 1048576) return (n / 1048576).toFixed(1) + ' MiB';
  if (n >= 1024) return (n / 1024).toFixed(1) + ' KiB';
  return n.toFixed(0) + ' B';
}

function fmtRate(n) {
  if (n >= 1073741824) return (n / 1073741824).toFixed(1) + ' GiB/s';
  if (n >= 1048576) return (n / 1048576).toFixed(1) + ' MiB/s';
  if (n >= 1024) return (n / 1024).toFixed(1) + ' KiB/s';
  return n.toFixed(0) + ' B/s';
}

function barWidth(rate, maxRate) {
  if (maxRate <= 0) return 0;
  return Math.min(100, (rate / maxRate) * 100);
}

function maxRate(backends) {
  let m = 0;
  for (const b of backends) {
    if (b.rate_out > m) m = b.rate_out;
    if (b.rate_in > m) m = b.rate_in;
  }
  return m || 1;
}

function toggleConns(id) {
  const rows = document.querySelectorAll('.detail-conns-' + id);
  for (const r of rows) r.classList.toggle('show');
}

async function refresh() {
  try {
    const resp = await fetch(API + '/frontends');
    const data = await resp.json();
    render(data.frontends || []);
  } catch (e) {
    document.getElementById('content').innerHTML = '<p class="loading">Connection error</p>';
  }
}

function render(frontends) {
  let html = '';
  for (const f of frontends) {
    const mr = maxRate(f.backends);
    html += '<h2>Frontend: ' + f.listen_addr + ' <span class="tag active">' + f.active_connections + ' conns</span></h2>';
    html += '<table><thead><tr><th>Backend</th><th>Conns</th><th>In</th><th>Out</th><th>In Rate</th><th>Out Rate</th></tr></thead><tbody>';
    for (const b of f.backends) {
      html += '<tr onclick="toggleConns(\'' + f.id + '\')" style="cursor:pointer">';
      html += '<td>' + b.addr + '</td>';
      html += '<td class="num">' + b.active_connections + '</td>';
      html += '<td class="num">' + fmt(b.bytes_in) + '</td>';
      html += '<td class="num">' + fmt(b.bytes_out) + '</td>';
      html += '<td class="num">' + fmtRate(b.rate_in) + ' <div class="bar-wrap"><div class="bar-fill" style="width:' + barWidth(b.rate_in, mr) + '%"></div></div></td>';
      html += '<td class="num">' + fmtRate(b.rate_out) + ' <div class="bar-wrap"><div class="bar-fill out" style="width:' + barWidth(b.rate_out, mr) + '%"></div></div></td>';
      html += '</tr>';
    }
    html += '<tr><td><strong>Total</strong></td>';
    html += '<td class="num">' + f.active_connections + '</td>';
    html += '<td class="num">' + fmt(f.bytes_in) + '</td>';
    html += '<td class="num">' + fmt(f.bytes_out) + '</td>';
    html += '<td class="num">' + fmtRate(f.rate_in) + '</td>';
    html += '<td class="num">' + fmtRate(f.rate_out) + '</td></tr>';
    html += '</tbody></table>';
  }
  if (frontends.length === 0) {
    html = '<p class="loading">No frontends configured.</p>';
  }
  document.getElementById('content').innerHTML = html;
}

setInterval(refresh, 1000);
refresh();
</script>
</body>
</html>
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./...`
Expected: no errors (index.html now exists for embedding).

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: embedded HTML dashboard with real-time API polling"
```

---

### Task 7: Wire everything together in main.go

**Files:**
- Modify: `main.go` — replace the stub proxy call with actual logic.

Up to this point the `main.go` already contains the Proxy instantiation and Start() call from Task 1. The proxy.go's `Start()` calls `p.startAPI()` which is defined in api.go. Everything should be wired.

- [ ] **Step 1: Verify full build**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 2: Test with echo server**

Start echo server: `go run tests/echo_server.go &` (or use netcat)

```bash
go run main.go -frontend :8080 -backend :9999 -lb least_conn -api :9090 &
sleep 1
echo "hello" | nc -w 2 localhost 8080
```

Expected: proxy starts, connection is proxied, API responds at localhost:9090/api/frontends.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: integrate all components - fully functional proxy"
```

---

### Task 8: Final integration test and edge case handling

- [ ] **Step 1: Add timeout and error handling for backend connections**

In `proxy.go`, the `handleConnection` already has a `DialTimeout` of 10s. Add a `recover()` in each goroutine to prevent panics from crashing the proxy.

```go
func (p *Proxy) handleConnection(clientConn net.Conn, cfg FrontendConfig, fs *frontendStats) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in handleConnection: %v\n", r)
		}
	}()
	// ... existing code
}
```

Also add recover in `pipe`:

```go
func (p *Proxy) pipe(dst io.Writer, src io.Reader, cs *connStats, total, sampling *atomic.Int64, done chan bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in pipe: %v\n", r)
			done <- true
		}
	}()
	// ... existing code
}
```

- [ ] **Step 2: Verify build**

Run: `go build ./...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "fix: add panic recovery in proxy handlers"
```
