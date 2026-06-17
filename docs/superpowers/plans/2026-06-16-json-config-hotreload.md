# JSON Config + Hot Reload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add JSON config file support with hot reload (no restart needed) to kbproxy.

**Architecture:** New `-config` flag loads a JSON file that reuses existing URL-format parsing. A polling goroutine watches the file's mtime and triggers `ReloadConfig` on change. Proxy gains dynamic frontend/backend management with graceful draining. Config mode and CLI flags are mutually exclusive.

**Tech Stack:** Go 1.22, standard library only (`encoding/json`, `os`, `time`)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `config.go` | Create | JSON structs, file loading, URL parsing bridge |
| `reload.go` | Create | Config file mtime polling goroutine |
| `proxy.go` | Modify | Dynamic frontend management, `ReloadConfig`, draining |
| `healthcheck.go` | Modify | Context-based cancellation for health checks |
| `stats.go` | Modify | `draining` field on `backendStats`, `unregisterFrontend`, dynamic backend add/remove |
| `main.go` | Modify | `-config` flag, mutual exclusion logic, config mode entry |
| `api.go` | Modify | Hot-reload status endpoint |
| `README.md` | Modify | Document config file usage and hot reload |

---

### Task 1: Config file loading (config.go)

**Files:**
- Create: `config.go`

- [ ] **Step 1: Create config.go with JSON structs and loader**

```go
package main

import (
	"encoding/json"
	"fmt"
	"os"
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
```

Note: `configToProxyConfigs` uses existing `parseFrontendURL`, `parseBackends`, `strings.Join`. The `"strings"` import is already used in `main.go`.

- [ ] **Step 2: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 3: Commit**

```bash
git add config.go
git commit -m "feat: add JSON config file loading (config.go)"
```

---

### Task 2: Health check cancellation (healthcheck.go)

Health checks currently run forever via `for range ticker.C`. We need to make them cancellable so removed backends stop their checks.

**Files:**
- Modify: `healthcheck.go`

- [ ] **Step 1: Add context support to startHealthCheck**

Replace the entire `startHealthCheck` function:

```go
func startHealthCheck(ctx context.Context, bs *backendStats, script string, interval, timeout time.Duration) {
	host, port, err := net.SplitHostPort(bs.addr)
	if err != nil {
		fmt.Printf("[health] ERROR: cannot parse backend addr %q: %v\n", bs.addr, err)
		return
	}

	go func() {
		jitter := time.Duration(rand.Int63n(int64(interval)))
		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		}
		runCheck(bs, script, host, port, timeout)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				runCheck(bs, script, host, port, timeout)
			case <-ctx.Done():
				return
			}
		}
	}()
}
```

- [ ] **Step 2: Update the call site in stats.go**

In `stats.go`, `registerFrontend` calls `startHealthCheck`. We need to pass a context. For now, we'll use `context.Background()` — this will be replaced in Task 4 when we add proper context management.

Change line 158 in `stats.go` from:
```go
startHealthCheck(bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
```
to:
```go
startHealthCheck(context.Background(), bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
```

Add `"context"` to the imports in `stats.go`.

- [ ] **Step 3: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 4: Commit**

```bash
git add healthcheck.go stats.go
git commit -m "feat: add context-based cancellation to health checks"
```

---

### Task 3: Backend draining and stats dynamic management (stats.go)

**Files:**
- Modify: `stats.go`

- [ ] **Step 1: Add draining field to backendStats**

Add a `draining atomic.Bool` field to `backendStats` struct (after the `healthy` field at line 91):

```go
	draining        atomic.Bool
```

- [ ] **Step 2: Add unregisterFrontend method to statsCollector**

Add after `registerFrontend`:

```go
func (sc *statsCollector) unregisterFrontend(id string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.frontends, id)
}
```

- [ ] **Step 3: Add updateFrontendBackends method to statsCollector**

This method handles backend add/remove/update for a frontend:

```go
func (sc *statsCollector) updateFrontendBackends(fs *frontendStats, backendConfigs []BackendConfig, cancelFuncs map[string]context.CancelFunc) (newCancelFuncs map[string]context.CancelFunc) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newCancelFuncs = make(map[string]context.CancelFunc)

	existing := make(map[string]*backendStats)
	for _, bs := range fs.backends {
		existing[bs.addr] = bs
	}

	wanted := make(map[string]BackendConfig)
	for _, bc := range backendConfigs {
		wanted[bc.Addr] = bc
	}

	var newBackends []*backendStats
	for _, bc := range backendConfigs {
		if bs, ok := existing[bc.Addr]; ok {
			bs.weight = bc.Weight
			bs.backup = bc.Backup
			if bc.CheckScript != "" {
				bs.checkInterval.Store(bc.CheckInterval.Milliseconds())
				if cancel, ok := cancelFuncs[bc.Addr]; ok {
					newCancelFuncs[bc.Addr] = cancel
				} else {
					ctx, cancel := context.WithCancel(context.Background())
					newCancelFuncs[bc.Addr] = cancel
					startHealthCheck(ctx, bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
				}
			} else {
				if cancel, ok := cancelFuncs[bc.Addr]; ok {
					cancel()
					delete(cancelFuncs, bc.Addr)
				}
			}
			bs.draining.Store(false)
			newBackends = append(newBackends, bs)
		} else {
			bs := newBackendStats(bc.Addr, bc.Weight, bc.Backup)
			if bc.CheckScript != "" {
				bs.checkInterval.Store(bc.CheckInterval.Milliseconds())
				ctx, cancel := context.WithCancel(context.Background())
				newCancelFuncs[bc.Addr] = cancel
				startHealthCheck(ctx, bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
			}
			newBackends = append(newBackends, bs)
		}
	}

	for addr, bs := range existing {
		if _, ok := wanted[addr]; !ok {
			bs.draining.Store(true)
			if cancel, ok := cancelFuncs[addr]; ok {
				cancel()
			}
			if bs.activeConns.Load() == 0 {
				continue
			}
			newBackends = append(newBackends, bs)
		}
	}

	fs.backends = newBackends
	return newCancelFuncs
}
```

Add `"context"` to imports (already added in Task 2).

- [ ] **Step 4: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 5: Commit**

```bash
git add stats.go
git commit -m "feat: add backend draining and dynamic stats management"
```

---

### Task 4: Proxy dynamic management (proxy.go)

**Files:**
- Modify: `proxy.go`

- [ ] **Step 1: Add frontendListener struct and update Proxy struct**

Add before the `Proxy` struct definition:

```go
type frontendListener struct {
	cfg     FrontendConfig
	fs      *frontendStats
	lb      loadBalancer
	cancel  context.CancelFunc
	done    chan struct{}
	draining atomic.Bool
}
```

Update the `Proxy` struct to add dynamic management fields:

```go
type Proxy struct {
	configs     []FrontendConfig
	apiAddr     string
	apiUser     string
	apiPass     string
	stats       *statsCollector
	lbCache     map[string]loadBalancer
	connID      atomic.Int64
	frontendID  atomic.Int64

	mu          sync.Mutex
	frontends   map[string]*frontendListener
	hcCancels   map[string]map[string]context.CancelFunc
	configPath  string
}
```

Add `"context"` and `"sync"` to imports in `proxy.go`. Remove `"sync/atomic"` if it becomes unused (it is used by `atomic.Int64` so keep it).

- [ ] **Step 2: Update NewProxy to initialize new fields**

```go
func NewProxy(configs []FrontendConfig, apiAddr, apiUser, apiPass string) *Proxy {
	return &Proxy{
		configs:   configs,
		apiAddr:   apiAddr,
		apiUser:   apiUser,
		apiPass:   apiPass,
		stats:     newStatsCollector(),
		lbCache:   make(map[string]loadBalancer),
		frontends: make(map[string]*frontendListener),
		hcCancels: make(map[string]map[string]context.CancelFunc),
	}
}
```

- [ ] **Step 3: Update Start method to use addFrontend**

```go
func (p *Proxy) Start() error {
	p.stats.startSampling()

	for _, cfg := range p.configs {
		p.addFrontend(cfg)
	}

	return p.startAPI()
}
```

- [ ] **Step 4: Add addFrontend method**

Note: `stats.registerFrontend` must NOT start health checks (we'll manage them in `addFrontend`). Replace `registerFrontend` in `stats.go` with:

```go
func (sc *statsCollector) registerFrontend(id, listenAddr string, rateLimit int64, backendConfigs []BackendConfig) *frontendStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	fs := newFrontendStats(id, listenAddr)
	fs.rateLimit = rateLimit
	for _, bc := range backendConfigs {
		bs := newBackendStats(bc.Addr, bc.Weight, bc.Backup)
		if bc.CheckScript != "" {
			bs.checkInterval.Store(bc.CheckInterval.Milliseconds())
		}
		fs.backends = append(fs.backends, bs)
	}
	sc.frontends[id] = fs
	return fs
}
```

Now add `addFrontend` and `findBackend` in `proxy.go`:

```go
func findBackend(fs *frontendStats, addr string) (int, *backendStats) {
	for i, bs := range fs.backends {
		if bs.addr == addr {
			return i, bs
		}
	}
	return -1, nil
}

func (p *Proxy) addFrontend(cfg FrontendConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	id := fmt.Sprintf("f-%d", p.frontendID.Add(1)-1)
	fs := p.stats.registerFrontend(id, cfg.ListenAddr, cfg.RateLimit, cfg.Backends)
	lb := newLoadBalancer(cfg.LBStrategy)
	p.lbCache[id] = lb

	hcCancels := make(map[string]context.CancelFunc)
	for _, bc := range cfg.Backends {
		if bc.CheckScript != "" {
			_, bs := findBackend(fs, bc.Addr)
			if bs != nil {
				childCtx, childCancel := context.WithCancel(ctx)
				hcCancels[bc.Addr] = childCancel
				startHealthCheck(childCtx, bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
			}
		}
	}

	fl := &frontendListener{
		cfg:    cfg,
		fs:     fs,
		lb:     lb,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	p.mu.Lock()
	p.frontends[cfg.ListenAddr] = fl
	p.hcCancels[cfg.ListenAddr] = hcCancels
	p.mu.Unlock()

	go p.listenFrontend(cfg, fs, fl)
}
```

Wait — `stats.registerFrontend` already calls `startHealthCheck`. We need to change that. Let's update `registerFrontend` in `stats.go` to NOT start health checks, and handle it in `addFrontend` instead.

Actually, let's take a different approach. Change `registerFrontend` to not start health checks. Update `stats.go`:

Replace `registerFrontend` with:

```go
func (sc *statsCollector) registerFrontend(id, listenAddr string, rateLimit int64, backendConfigs []BackendConfig) *frontendStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	fs := newFrontendStats(id, listenAddr)
	fs.rateLimit = rateLimit
	for _, bc := range backendConfigs {
		bs := newBackendStats(bc.Addr, bc.Weight, bc.Backup)
		if bc.CheckScript != "" {
			bs.checkInterval.Store(bc.CheckInterval.Milliseconds())
		}
		fs.backends = append(fs.backends, bs)
	}
	sc.frontends[id] = fs
	return fs
}
```

Now add the helper function `findBackend` in `proxy.go`:

```go
func findBackend(fs *frontendStats, addr string) (int, *backendStats) {
	for i, bs := range fs.backends {
		if bs.addr == addr {
			return i, bs
		}
	}
	return -1, nil
}
```

- [ ] **Step 5: Update listenFrontend to accept frontendListener**

Change signature from:
```go
func (p *Proxy) listenFrontend(cfg FrontendConfig, fs *frontendStats) {
```
to:
```go
func (p *Proxy) listenFrontend(cfg FrontendConfig, fs *frontendStats, fl *frontendListener) {
```

After `listener.Close()` in the defer, close the done channel:

```go
func (p *Proxy) listenFrontend(cfg FrontendConfig, fs *frontendStats, fl *frontendListener) {
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[proxy] FATAL: listen %s: %v\n", cfg.ListenAddr, err)
		os.Exit(1)
	}
	defer func() {
		listener.Close()
		close(fl.done)
	}()

	fmt.Printf("[proxy] listening on %s (frontend: %s)\n", cfg.ListenAddr, fs.id)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if fl.draining.Load() {
				return
			}
			fmt.Printf("[proxy] accept error on %s: %v\n", cfg.ListenAddr, err)
			continue
		}
		go p.handleConnection(clientConn, cfg, fs)
	}
}
```

- [ ] **Step 6: Add removeFrontend method**

```go
func (p *Proxy) removeFrontend(addr string) {
	p.mu.Lock()
	fl, ok := p.frontends[addr]
	if !ok {
		p.mu.Unlock()
		return
	}
	delete(p.frontends, addr)
	if cancels, ok := p.hcCancels[addr]; ok {
		for _, cancel := range cancels {
			cancel()
		}
		delete(p.hcCancels, addr)
	}
	p.mu.Unlock()

	fl.draining.Store(true)
	fl.cancel()

	fmt.Printf("[proxy] draining frontend %s, waiting for connections to close...\n", addr)

	go func() {
		select {
		case <-fl.done:
		case <-time.After(30 * time.Second):
			fmt.Printf("[proxy] frontend %s: graceful shutdown timed out\n", addr)
		}
		p.stats.unregisterFrontend(fl.fs.id)
		delete(p.lbCache, fl.fs.id)
		fmt.Printf("[proxy] frontend %s removed\n", addr)
	}()
}
```

- [ ] **Step 7: Add ReloadConfig method**

```go
func (p *Proxy) ReloadConfig(configs []FrontendConfig) {
	p.mu.Lock()
	currentAddrs := make(map[string]bool)
	for addr := range p.frontends {
		currentAddrs[addr] = true
	}
	p.mu.Unlock()

	newAddrs := make(map[string]bool)
	for _, cfg := range configs {
		newAddrs[cfg.ListenAddr] = true
	}

	for addr := range currentAddrs {
		if !newAddrs[addr] {
			p.removeFrontend(addr)
		}
	}

	for _, cfg := range configs {
		p.mu.Lock()
		fl, exists := p.frontends[cfg.ListenAddr]
		p.mu.Unlock()

		if !exists {
			p.addFrontend(cfg)
			continue
		}

		p.updateFrontendBackends(fl, cfg)
	}
}

func (p *Proxy) updateFrontendBackends(fl *frontendListener, cfg FrontendConfig) {
	fl.cfg = cfg
	fl.fs.rateLimit = cfg.RateLimit

	strategy := cfg.LBStrategy
	if strategy == "" {
		strategy = "least_bandwidth"
	}
	p.lbCache[fl.fs.id] = newLoadBalancer(strategy)

	p.mu.Lock()
	cancels := p.hcCancels[cfg.ListenAddr]
	if cancels == nil {
		cancels = make(map[string]context.CancelFunc)
	}
	p.mu.Unlock()

	newCancels := p.stats.updateFrontendBackends(fl.fs, cfg.Backends, cancels)

	p.mu.Lock()
	p.hcCancels[cfg.ListenAddr] = newCancels
	p.mu.Unlock()
}
```

- [ ] **Step 8: Update handleConnection to skip draining backends**

In `handleConnection`, after the `backup` split loop (around line 78-89), the draining backends are already excluded because `updateFrontendBackends` in stats.go marks them `draining` and the `Pick` function only gets non-draining backends. But we need to add the draining check in `handleConnection`:

After the `bs := lb.Pick(pickBackends)` line, the backend is already selected. The draining check should happen at the load balancer Pick level. Actually, we need to filter draining backends in `handleConnection`. Change the backend filtering loop:

```go
	var primary []*backendStats
	var backup []*backendStats
	for _, b := range fs.backends {
		if b.draining.Load() || !b.healthy.Load() {
			continue
		}
		if b.backup {
			backup = append(backup, b)
		} else {
			primary = append(primary, b)
		}
	}
```

- [ ] **Step 9: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 10: Commit**

```bash
git add proxy.go stats.go
git commit -m "feat: add dynamic frontend/backend management with graceful draining"
```

---

### Task 5: Config file watcher (reload.go)

**Files:**
- Create: `reload.go`

- [ ] **Step 1: Create reload.go**

```go
package main

import (
	"fmt"
	"os"
	"time"
)

func watchConfigFile(path string, interval time.Duration, onChange func() error) {
	go func() {
		var lastMod time.Time
		if info, err := os.Stat(path); err == nil {
			lastMod = info.ModTime()
		}

		for range time.NewTicker(interval).C {
			info, err := os.Stat(path)
			if err != nil {
				fmt.Printf("[reload] ERROR: stat %s: %v\n", path, err)
				continue
			}
			if info.ModTime().After(lastMod) {
				lastMod = info.ModTime()
				fmt.Printf("[reload] config file changed, reloading...\n")
				if err := onChange(); err != nil {
					fmt.Printf("[reload] ERROR: %v\n", err)
				} else {
					fmt.Printf("[reload] config reloaded successfully\n")
				}
			}
		}
	}()
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 3: Commit**

```bash
git add reload.go
git commit -m "feat: add config file mtime polling watcher"
```

---

### Task 6: Integrate config mode in main.go

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Add -config flag and mutual exclusion logic**

Replace the entire `main` function:

```go
func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: kbproxy [flags]\n\nFlags:\n")
		flag.PrintDefaults()
	}

	var apiAddr string
	var apiUser string
	var apiPass string
	var configPath string

	flag.StringVar(&apiAddr, "api", ":9090", "API server listen address")
	flag.StringVar(&apiUser, "api-user", "", "API Basic Auth username")
	flag.StringVar(&apiPass, "api-pass", "", "API Basic Auth password")
	flag.StringVar(&configPath, "config", "", "JSON config file path (enables hot reload)")

	var frontendURLs multiFlag
	var backendLists multiFlag

	flag.Var(&frontendURLs, "frontend", "Frontend URL: tcp://:8080?lb=least_conn (repeatable)")
	flag.Var(&backendLists, "backend", "Comma-separated backend URLs: tcp://10.0.0.1:80,tcp://10.0.0.2:80")

	flag.Parse()

	if configPath != "" {
		if len(frontendURLs) > 0 || len(backendLists) > 0 {
			fmt.Fprintf(os.Stderr, "Error: -config cannot be used with -frontend or -backend\n")
			os.Exit(1)
		}

		cf, err := loadConfigFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		configs, err := configToProxyConfigs(cf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		proxy := NewProxy(configs, cf.API, cf.APIUser, cf.APIPass)
		proxy.configPath = configPath
		watchConfigFile(configPath, 5*time.Second, func() error {
			cf, err := loadConfigFile(configPath)
			if err != nil {
				return err
			}
			configs, err := configToProxyConfigs(cf)
			if err != nil {
				return err
			}
			if cf.API != proxy.apiAddr {
				fmt.Printf("[reload] WARNING: api address change (%s -> %s) requires restart, ignoring\n", proxy.apiAddr, cf.API)
			}
			proxy.apiUser = cf.APIUser
			proxy.apiPass = cf.APIPass
			proxy.ReloadConfig(configs)
			return nil
		})
		if err := proxy.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		return
	}

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
```

- [ ] **Step 2: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 3: Commit**

```bash
git add main.go
git commit -m "feat: integrate -config flag with mutual exclusion and hot reload"
```

---

### Task 7: Hot reload status API (api.go)

**Files:**
- Modify: `api.go`

- [ ] **Step 1: Add reload status endpoint**

Add a handler in `startAPI` (after the test endpoint registration):

In the `if p.apiUser != ""` block add:
```go
		mux.HandleFunc("/api/reload", p.basicAuth(p.handleReload))
```

In the `else` block add:
```go
		mux.HandleFunc("/api/reload", p.handleReload)
```

Add the handler function:

```go
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
```

- [ ] **Step 2: Verify compilation**

Run: `go build -o /dev/null .`
Expected: compiles without errors

- [ ] **Step 3: Commit**

```bash
git add api.go
git commit -m "feat: add /api/reload endpoint for manual config reload trigger"
```

---

### Task 8: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add config file documentation**

After the "健康检查" section (around line 87), add:

```markdown
### 配置文件

使用 JSON 配置文件代替命令行参数，支持热重载（修改文件后自动生效，无需重启）：

```bash
kbproxy -config kbproxy.json
```

配置文件格式：

```json
{
  "api": ":9090",
  "api_user": "",
  "api_pass": "",
  "frontends": [
    {
      "url": "tcp://:8080?lb=least_conn",
      "backends": ["tcp://10.0.0.1:80?weight=3", "tcp://10.0.0.2:80?backup"]
    }
  ]
}
```

`frontends[].url` 和 `backends[]` 的参数格式与命令行一致。

**热重载：**

- 修改配置文件后 5 秒内自动生效
- 新增前端：立即开始监听
- 移除前端：停止接受新连接，已有连接优雅等待关闭（最多 30 秒）
- 后端增删改：即时生效，移除的后端不再接受新连接
- API 地址变更不支持热重载，需重启
- 也可通过 `POST /api/reload` 手动触发重载

**注意：** `-config` 与 `-frontend`/`-backend` 互斥，不能同时使用。
```

Also add `/api/reload` to the API table:

```markdown
| `POST /api/reload` | 手动触发配置重载（需 -config 模式） |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add config file and hot reload documentation"
```

---

### Task 9: Integration test

**Files:**
- None (manual testing)

- [ ] **Step 1: Create a test config file**

Write `/tmp/kbproxy-test.json`:

```json
{
  "api": ":9090",
  "frontends": [
    {
      "url": "tcp://:8080",
      "backends": ["tcp://127.0.0.1:8888"]
    }
  ]
}
```

- [ ] **Step 2: Test config mode startup**

```bash
go build -o /tmp/kbproxy-test . && /tmp/kbproxy-test -config /tmp/kbproxy-test.json &
sleep 1
curl -s http://localhost:9090/api/stats | python3 -m json.tool
```

Expected: stats API returns the frontend with backend 127.0.0.1:8888

- [ ] **Step 3: Test hot reload - add backend**

Update `/tmp/kbproxy-test.json`:

```json
{
  "api": ":9090",
  "frontends": [
    {
      "url": "tcp://:8080",
      "backends": ["tcp://127.0.0.1:8888", "tcp://127.0.0.1:8889"]
    }
  ]
}
```

Wait 6 seconds, then:

```bash
curl -s http://localhost:9090/api/stats | python3 -m json.tool
```

Expected: two backends listed

- [ ] **Step 4: Test hot reload - remove frontend**

Update `/tmp/kbproxy-test.json`:

```json
{
  "api": ":9090",
  "frontends": [
    {
      "url": "tcp://:9091",
      "backends": ["tcp://127.0.0.1:8888"]
    }
  ]
}
```

Wait 6 seconds, then:

```bash
curl -s http://localhost:9090/api/stats | python3 -m json.tool
```

Expected: frontend :8080 gone, :9091 present

- [ ] **Step 5: Test mutual exclusion**

```bash
/tmp/kbproxy-test -config /tmp/kbproxy-test.json -frontend tcp://:8080
```

Expected: error message about mutual exclusion

- [ ] **Step 6: Test /api/reload endpoint**

```bash
curl -X POST http://localhost:9090/api/reload
```

Expected: `{"status":"ok"}`

- [ ] **Step 7: Cleanup**

```bash
kill %1 2>/dev/null; rm /tmp/kbproxy-test /tmp/kbproxy-test.json
```

- [ ] **Step 8: Final build verification**

```bash
go build -o /dev/null .
go vet ./...
```

Expected: no errors
