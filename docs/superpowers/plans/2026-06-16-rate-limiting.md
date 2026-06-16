# Rate Limiting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-connection, per-frontend rate limiting to kbproxy using a sleep-based approach, configured via the `rate_limit` URL parameter.

**Architecture:** Frontend URL parameter `rate_limit` (e.g. `tcp://:8080?rate_limit=10m`) is parsed at startup. The rate limit value is passed into each connection's `pipe()` call, where a simple sleep-based throttling calculates the expected time for the bytes written and sleeps to fill the gap if needed.

**Tech Stack:** Go 1.22, standard library only (zero external dependencies)

---

### Task 1: Add RateLimit field and parseRateLimit function to main.go

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Add `RateLimit` field to `FrontendConfig`**

In `main.go`, add `RateLimit int64` field to `FrontendConfig` struct (after `LBStrategy`):

```go
type FrontendConfig struct {
	ListenAddr string
	Backends   []BackendConfig
	LBStrategy string
	RateLimit  int64
}
```

- [ ] **Step 2: Add `parseRateLimit` function**

Add after the `parseBackendURL` function (before `main()`):

```go
func parseRateLimit(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	s = strings.TrimSpace(strings.ToLower(s))
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "g"):
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	case strings.HasSuffix(s, "k"):
		multiplier = 1024
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("invalid rate_limit %q: must be a positive number with optional k/m/g suffix", s)
	}
	return v * multiplier, nil
}
```

- [ ] **Step 3: Parse `rate_limit` in `parseFrontendURL`**

In `parseFrontendURL`, after parsing `lb`, add parsing of `rate_limit`:

Change the function to:

```go
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
	rateLimit, err := parseRateLimit(u.Query().Get("rate_limit"))
	if err != nil {
		return FrontendConfig{}, fmt.Errorf("invalid frontend URL %q: %w", raw, err)
	}
	return FrontendConfig{ListenAddr: host, LBStrategy: lb, RateLimit: rateLimit}, nil
}
```

- [ ] **Step 4: Build and verify**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 5: Commit**

```bash
git add main.go
git commit -m "feat: add RateLimit field and parseRateLimit to FrontendConfig"
```

---

### Task 2: Implement sleep-based rate limiting in pipe()

**Files:**
- Modify: `proxy.go`

- [ ] **Step 1: Add `rateLimit` parameter to `pipe()` and implement throttling**

Change the `pipe` function signature and body. The new `pipe` function:

```go
func (p *Proxy) pipe(dst io.Writer, src io.Reader, cs *connStats, connTotal, connSampling, backendTotal, frontendTotal *atomic.Int64, rateLimit int64, done chan bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in pipe: %v\n%s", r, debug.Stack())
			done <- true
		}
	}()
	buf := make([]byte, 32*1024)
	var byteCount int64
	start := time.Now()
	for {
		n, err := src.Read(buf)
		if n > 0 {
			connTotal.Add(int64(n))
			connSampling.Add(int64(n))
			if backendTotal != nil {
				backendTotal.Add(int64(n))
			}
			if frontendTotal != nil {
				frontendTotal.Add(int64(n))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				done <- true
				return
			}
			if rateLimit > 0 {
				byteCount += int64(n)
				expected := time.Duration(byteCount * int64(time.Second) / rateLimit)
				elapsed := time.Since(start)
				if expected > elapsed {
					time.Sleep(expected - elapsed)
				}
			}
		}
		if err != nil {
			done <- true
			return
		}
	}
}
```

- [ ] **Step 2: Update `handleConnection` to pass `rateLimit` to `pipe()`**

In `handleConnection`, change the two `go p.pipe(...)` calls to include `cfg.RateLimit`:

Line 122 changes from:
```go
go p.pipe(clientConn, backendConn, cs, &cs.bytesOut, &cs.samplingBytesOut, &cs.bs.bytesIn, &cs.fs.bytesIn, done)
```
to:
```go
go p.pipe(clientConn, backendConn, cs, &cs.bytesOut, &cs.samplingBytesOut, &cs.bs.bytesIn, &cs.fs.bytesIn, cfg.RateLimit, done)
```

Line 123 changes from:
```go
go p.pipe(backendConn, clientConn, cs, &cs.bytesIn, &cs.samplingBytesIn, &cs.bs.bytesOut, &cs.fs.bytesOut, done)
```
to:
```go
go p.pipe(backendConn, clientConn, cs, &cs.bytesIn, &cs.samplingBytesIn, &cs.bs.bytesOut, &cs.fs.bytesOut, cfg.RateLimit, done)
```

- [ ] **Step 3: Build and verify**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 4: Commit**

```bash
git add proxy.go
git commit -m "feat: implement sleep-based per-connection rate limiting in pipe()"
```

---

### Task 3: Expose rate_limit in API stats response

**Files:**
- Modify: `stats.go`
- Modify: `api.go`

- [ ] **Step 1: Add `RateLimit` field to `frontendStats`**

In `stats.go`, add `rateLimit int64` field to `frontendStats` struct (after `backends`):

```go
type frontendStats struct {
	id          string
	listenAddr  string
	bytesIn     atomic.Int64
	bytesOut    atomic.Int64
	rateIn      *slidingWindow
	rateOut     *slidingWindow
	activeConns atomic.Int64
	totalConns  atomic.Int64
	peakConns   atomic.Int64
	peakRateIn  atomic.Int64
	peakRateOut atomic.Int64
	backends    []*backendStats
	rateLimit   int64
}
```

- [ ] **Step 2: Update `registerFrontend` to accept and store `rateLimit`**

Change `registerFrontend` signature and body:

```go
func (sc *statsCollector) registerFrontend(id, listenAddr string, rateLimit int64, backendConfigs []BackendConfig) *frontendStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	fs := newFrontendStats(id, listenAddr)
	fs.rateLimit = rateLimit
	for _, bc := range backendConfigs {
		bs := newBackendStats(bc.Addr, bc.Weight, bc.Backup)
		fs.backends = append(fs.backends, bs)
		if bc.CheckScript != "" {
			bs.checkInterval.Store(bc.CheckInterval.Milliseconds())
			startHealthCheck(bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
		}
	}
	sc.frontends[id] = fs
	return fs
}
```

- [ ] **Step 3: Update `Proxy.Start()` call to `registerFrontend`**

In `proxy.go`, change line 39 from:
```go
fs := p.stats.registerFrontend(id, cfg.ListenAddr, cfg.Backends)
```
to:
```go
fs := p.stats.registerFrontend(id, cfg.ListenAddr, cfg.RateLimit, cfg.Backends)
```

- [ ] **Step 4: Add `RateLimit` field to `frontendStat` JSON struct in `api.go`**

In `api.go`, add `RateLimit` field to `frontendStat`:

```go
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
```

- [ ] **Step 5: Populate `RateLimit` in `handleStats`**

In `api.go`, in the `handleStats` function, update the `frontendStat` construction to include `RateLimit`:

```go
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
```

- [ ] **Step 6: Build and verify**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 7: Commit**

```bash
git add stats.go api.go proxy.go
git commit -m "feat: expose rate_limit in API stats response"
```

---

### Task 4: Manual smoke test

**Files:** None (testing only)

- [ ] **Step 1: Build the binary**

Run: `go build -o kbproxy .`

- [ ] **Step 2: Test without rate_limit (should behave as before)**

Run in one terminal:
```bash
./kbproxy -frontend tcp://:8080 -backend tcp://127.0.0.1:9999 -api :9090
```

Verify it starts and the dashboard loads at http://localhost:9090.

- [ ] **Step 3: Test with rate_limit parameter**

Run in one terminal (start a simple backend):
```bash
python3 -m http.server 9999
```

Run in another terminal:
```bash
./kbproxy -frontend "tcp://:8081?rate_limit=1m" -backend tcp://127.0.0.1:9999 -api :9091
```

Run in a third terminal, download a file through the proxy and verify speed is ~1MB/s:
```bash
curl -o /dev/null -w "Speed: %{speed_download} bytes/s\n" http://127.0.0.1:8081/
```

Expected: download speed should be approximately 1MB/s (around 1,000,000 bytes/s), significantly slower than without rate limiting.

- [ ] **Step 4: Test with k suffix**

```bash
./kbproxy -frontend "tcp://:8082?rate_limit=500k" -backend tcp://127.0.0.1:9999 -api :9092
```

Verify it starts without error.

- [ ] **Step 5: Test with invalid rate_limit**

```bash
./kbproxy -frontend "tcp://:8083?rate_limit=abc" -backend tcp://127.0.0.1:9999 -api :9093
```

Expected: should print an error and exit.

- [ ] **Step 6: Final commit (if any fixups needed)**

If any issues were found and fixed during smoke testing, commit the fixes.
