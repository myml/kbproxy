# 外部健康检查实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 kbproxy 添加外部健康检查功能，后端可配置检查脚本，脚本周期执行并通过环境变量获取后端地址，检查失败的后端排除出负载均衡。

**Architecture:** 在现有 BackendConfig 中扩展查询参数（check/inter/check_timeout），在 backendStats 中添加 healthy 字段，启动独立 goroutine 执行健康检查，负载均衡 Pick 时过滤不健康后端，API 和页面展示健康状态。

**Tech Stack:** Go 1.22, os/exec, context, net

---

### Task 1: 扩展 BackendConfig 和 parseBackendURL

**Files:**
- Modify: `main.go:12-58`

- [ ] **Step 1: 扩展 BackendConfig 结构体**

在 `main.go` 的 `BackendConfig` 中添加三个字段：

```go
type BackendConfig struct {
	Addr          string
	Weight        int
	CheckScript   string
	CheckInterval time.Duration
	CheckTimeout  time.Duration
}
```

需要在 import 中添加 `"time"`。

- [ ] **Step 2: 修改 parseBackendURL 解析新参数**

```go
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
		CheckScript:   checkScript,
		CheckInterval: checkInterval,
		CheckTimeout:  checkTimeout,
	}, nil
}
```

- [ ] **Step 3: 编译验证**

Run: `go build ./...`
Expected: 编译成功，无错误

- [ ] **Step 4: 提交**

```bash
git add main.go
git commit -m "feat: 扩展 BackendConfig 支持健康检查参数 check/inter/check_timeout"
```

---

### Task 2: backendStats 添加 healthy 字段

**Files:**
- Modify: `stats.go:78-99`

- [ ] **Step 1: 在 backendStats 中添加 healthy 字段**

```go
type backendStats struct {
	addr        string
	weight      int
	bytesIn     atomic.Int64
	bytesOut    atomic.Int64
	rateIn      *slidingWindow
	rateOut     *slidingWindow
	activeConns atomic.Int64
	totalConns  atomic.Int64
	peakConns   atomic.Int64
	peakRateIn  atomic.Int64
	peakRateOut atomic.Int64
	healthy     atomic.Bool
}
```

- [ ] **Step 2: 在 newBackendStats 中初始化 healthy 为 true**

```go
func newBackendStats(addr string, weight int) *backendStats {
	bs := &backendStats{
		addr:    addr,
		weight:  weight,
		rateIn:  newSlidingWindow(),
		rateOut: newSlidingWindow(),
	}
	bs.healthy.Store(true)
	return bs
}
```

- [ ] **Step 3: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 4: 提交**

```bash
git add stats.go
git commit -m "feat: backendStats 添加 healthy 字段，默认为 true"
```

---

### Task 3: 实现健康检查 goroutine

**Files:**
- Create: `healthcheck.go`

- [ ] **Step 1: 创建 healthcheck.go**

```go
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"
)

func startHealthCheck(bs *backendStats, script string, interval, timeout time.Duration) {
	host, port, err := net.SplitHostPort(bs.addr)
	if err != nil {
		fmt.Printf("[health] ERROR: cannot parse backend addr %q: %v\n", bs.addr, err)
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			runCheck(bs, script, host, port, timeout)
		}
	}()
}

func runCheck(bs *backendStats, script, host, port string, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, script)
	cmd.Env = append(os.Environ(),
		"KBPROXY_BACKEND_HOST="+host,
		"KBPROXY_BACKEND_PORT="+port,
	)

	err := cmd.Run()
	wasHealthy := bs.healthy.Load()

	if ctx.Err() == context.DeadlineExceeded {
		if wasHealthy {
			fmt.Printf("[health] %s: check timeout (%v), marking DOWN\n", bs.addr, timeout)
		}
		bs.healthy.Store(false)
		return
	}

	if err != nil {
		if wasHealthy {
			fmt.Printf("[health] %s: check failed: %v, marking DOWN\n", bs.addr, err)
		}
		bs.healthy.Store(false)
		return
	}

	if !wasHealthy {
		fmt.Printf("[health] %s: check passed, marking UP\n", bs.addr)
	}
	bs.healthy.Store(true)
}
```

- [ ] **Step 2: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 3: 提交**

```bash
git add healthcheck.go
git commit -m "feat: 实现健康检查 goroutine，支持脚本执行和超时"
```

---

### Task 4: 在 registerFrontend 中启动健康检查

**Files:**
- Modify: `stats.go:138-147`

- [ ] **Step 1: 修改 registerFrontend 签名，传入 BackendConfig 并启动健康检查**

需要让 `registerFrontend` 能获取到 `CheckScript`、`CheckInterval`、`CheckTimeout`。修改为接收 `[]BackendConfig` 而不是仅 `[]BackendConfig` 的部分信息。

```go
func (sc *statsCollector) registerFrontend(id, listenAddr string, backendConfigs []BackendConfig) *frontendStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	fs := newFrontendStats(id, listenAddr)
	for _, bc := range backendConfigs {
		bs := newBackendStats(bc.Addr, bc.Weight)
		fs.backends = append(fs.backends, bs)
		if bc.CheckScript != "" {
			startHealthCheck(bs, bc.CheckScript, bc.CheckInterval, bc.CheckTimeout)
		}
	}
	sc.frontends[id] = fs
	return fs
}
```

- [ ] **Step 2: 验证调用方 proxy.go 中 registerFrontend 的调用无需修改**

`proxy.go:39` 调用 `p.stats.registerFrontend(id, cfg.ListenAddr, cfg.Backends)`，签名从 `([]BackendConfig)` 不变（只改了内部使用方式），无需修改调用方。

- [ ] **Step 3: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 4: 提交**

```bash
git add stats.go
git commit -m "feat: registerFrontend 中为配置了 check 的后端启动健康检查"
```

---

### Task 5: 负载均衡中过滤不健康后端

**Files:**
- Modify: `proxy.go:68-95`

- [ ] **Step 1: 在 handleConnection 中添加健康过滤逻辑**

修改 `handleConnection` 方法，在调用 `lb.Pick()` 前过滤不健康后端：

```go
func (p *Proxy) handleConnection(clientConn net.Conn, cfg FrontendConfig, fs *frontendStats) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in handleConnection: %v\n%s", r, debug.Stack())
		}
		clientConn.Close()
	}()

	lb := p.lbCache[fs.id]

	healthyBackends := make([]*backendStats, 0, len(fs.backends))
	for _, b := range fs.backends {
		if b.healthy.Load() {
			healthyBackends = append(healthyBackends, b)
		}
	}

	pickBackends := healthyBackends
	if len(pickBackends) == 0 {
		pickBackends = fs.backends
	}

	bs := lb.Pick(pickBackends)
	if bs == nil {
		fmt.Printf("[proxy] no backends available for %s\n", fs.id)
		return
	}

	bs.activeConns.Add(1)
	fs.activeConns.Add(1)
	bs.totalConns.Add(1)
	fs.totalConns.Add(1)

	backendConn, err := net.DialTimeout("tcp", bs.addr, 10*time.Second)
	if err != nil {
		bs.activeConns.Add(-1)
		fs.activeConns.Add(-1)
		fmt.Printf("[proxy] connect to backend %s: %v\n", bs.addr, err)
		return
	}
	defer backendConn.Close()

	connID := fmt.Sprintf("c-%d", p.connID.Add(1))
	cs := p.stats.registerConn(connID, fs, bs)

	fmt.Printf("[proxy] %s: %s <-> %s via %s\n", connID, clientConn.RemoteAddr(), bs.addr, fs.listenAddr)

	done := make(chan bool, 2)
	go p.pipe(clientConn, backendConn, cs, &cs.bytesOut, &cs.samplingBytesOut, &cs.bs.bytesIn, &cs.fs.bytesIn, done)
	go p.pipe(backendConn, clientConn, cs, &cs.bytesIn, &cs.samplingBytesIn, &cs.bs.bytesOut, &cs.fs.bytesOut, done)

	<-done
	clientConn.Close()
	backendConn.Close()
	<-done

	p.stats.unregisterConn(connID)
	fmt.Printf("[proxy] %s: closed (in=%d out=%d)\n", connID, cs.bytesIn.Load(), cs.bytesOut.Load())
}
```

- [ ] **Step 2: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 3: 提交**

```bash
git add proxy.go
git commit -m "feat: 负载均衡中过滤不健康后端，全部不健康时回退"
```

---

### Task 6: API 暴露健康状态

**Files:**
- Modify: `api.go:94-135`

- [ ] **Step 1: 在 backendStat 中添加 Healthy 字段**

```go
type backendStat struct {
	Addr        string `json:"addr"`
	Weight      int    `json:"weight"`
	TotalConns  int64  `json:"total_conns"`
	PeakConns   int64  `json:"peak_conns"`
	PeakRateIn  int64  `json:"peak_rate_in"`
	PeakRateOut int64  `json:"peak_rate_out"`
	Healthy     bool   `json:"healthy"`
}
```

- [ ] **Step 2: 在 handleStats 中填充 Healthy 字段**

修改 `handleStats` 中构建 `backendStat` 的代码：

```go
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
```

- [ ] **Step 3: 编译验证**

Run: `go build ./...`
Expected: 编译成功

- [ ] **Step 4: 提交**

```bash
git add api.go
git commit -m "feat: /api/stats 暴露后端 healthy 状态"
```

---

### Task 7: 前端页面展示健康状态

**Files:**
- Modify: `index.html:97-111`

- [ ] **Step 1: 在 buildBackendBlock 中展示健康状态标识**

修改 `buildBackendBlock` 函数，在 backend-header 中显示健康状态：

将原来的 header 行：
```javascript
  html += '<div class="backend-header'+(beCollapsed?' collapsed':'')+'" data-key="'+beKey+'" onclick="toggle(event)" title="total: '+beTotal+' | peak conns: '+bePeakConns+' | peak ↓: '+fmtRate(bePeakIn)+' | peak ↑: '+fmtRate(bePeakOut)+'"><span class="arrow">▼</span> '+be+' <span class="tag active">'+conns.length+' active</span> '+fmtRate(beRateIn)+'↓ '+fmtRate(beRateOut)+'↑</div>';
```

替换为：
```javascript
  const healthy = beS.healthy !== undefined ? beS.healthy : true;
  const healthTag = healthy ? '<span class="tag active">UP</span>' : '<span class="tag" style="background:#f38ba8;color:#11111b">DOWN</span>';
  html += '<div class="backend-header'+(beCollapsed?' collapsed':'')+'" data-key="'+beKey+'" onclick="toggle(event)" title="total: '+beTotal+' | peak conns: '+bePeakConns+' | peak ↓: '+fmtRate(bePeakIn)+' | peak ↑: '+fmtRate(bePeakOut)+'"><span class="arrow">▼</span> '+be+' '+healthTag+' <span class="tag active">'+conns.length+' active</span> '+fmtRate(beRateIn)+'↓ '+fmtRate(beRateOut)+'↑</div>';
```

- [ ] **Step 2: 手动验证**

Run: `go build -o kbproxy . && ./kbproxy -frontend "tcp://:8080" -backend "tcp://127.0.0.1:9999?check=/bin/true&inter=5" &`
在浏览器中打开 API 页面，确认后端显示 UP 标签。
停止进程。

- [ ] **Step 3: 提交**

```bash
git add index.html
git commit -m "feat: 前端页面展示后端健康状态 UP/DOWN 标识"
```

---

### Task 8: 集成测试验证

**Files:**
- No new files

- [ ] **Step 1: 测试健康检查脚本失败场景**

创建临时测试脚本：

```bash
# 创建一个返回失败的脚本
echo '#!/bin/bash\nexit 1' > /tmp/check_fail.sh && chmod +x /tmp/check_fail.sh
# 创建一个返回成功的脚本
echo '#!/bin/bash\nexit 0' > /tmp/check_ok.sh && chmod +x /tmp/check_ok.sh
```

启动 kbproxy 使用失败脚本：

```bash
go build -o kbproxy . && ./kbproxy -frontend "tcp://:18080" -backend "tcp://127.0.0.1:19999?check=/tmp/check_fail.sh&inter=2&check_timeout=3" -api :19090 &
```

等待 3 秒后检查 API：

```bash
sleep 3 && curl -s http://127.0.0.1:19090/api/stats | python3 -m json.tool
```

Expected: 后端 `healthy` 为 `false`，日志中看到 `marking DOWN`

- [ ] **Step 2: 测试健康检查脚本成功场景**

停止上一个进程，启动使用成功脚本的 kbproxy：

```bash
./kbproxy -frontend "tcp://:18080" -backend "tcp://127.0.0.1:19999?check=/tmp/check_ok.sh&inter=2&check_timeout=3" -api :19090 &
```

等待 3 秒后检查 API：

```bash
sleep 3 && curl -s http://127.0.0.1:19090/api/stats | python3 -m json.tool
```

Expected: 后端 `healthy` 为 `true`

- [ ] **Step 3: 测试无健康检查的后端**

停止上一个进程，启动不带 check 参数的 kbproxy：

```bash
./kbproxy -frontend "tcp://:18080" -backend "tcp://127.0.0.1:19999" -api :19090 &
```

检查 API：

```bash
sleep 1 && curl -s http://127.0.0.1:19090/api/stats | python3 -m json.tool
```

Expected: 后端 `healthy` 为 `true`（默认值）

- [ ] **Step 4: 清理测试进程和临时文件**

```bash
kill %1 2>/dev/null; rm -f /tmp/check_fail.sh /tmp/check_ok.sh
```

- [ ] **Step 5: 最终编译验证**

Run: `go build ./...`
Expected: 编译成功，无警告
