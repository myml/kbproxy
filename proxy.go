package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type frontendListener struct {
	cfg      FrontendConfig
	fs       *frontendStats
	lb       loadBalancer
	cancel   context.CancelFunc
	done     chan struct{}
	draining atomic.Bool
}

type Proxy struct {
	configs    []FrontendConfig
	apiAddr    string
	apiUser    string
	apiPass    string
	stats      *statsCollector
	lbCache    map[string]loadBalancer
	connID     atomic.Int64
	frontendID atomic.Int64

	mu         sync.Mutex
	frontends  map[string]*frontendListener
	hcCancels  map[string]map[string]context.CancelFunc
	configPath string
}

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

func (p *Proxy) Start() error {
	p.stats.startSampling()

	for _, cfg := range p.configs {
		p.addFrontend(cfg)
	}

	return p.startAPI()
}

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

func (p *Proxy) handleConnection(clientConn net.Conn, cfg FrontendConfig, fs *frontendStats) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in handleConnection: %v\n%s", r, debug.Stack())
		}
		clientConn.Close()
	}()

	lb := p.lbCache[fs.id]

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

	pickBackends := primary
	if len(pickBackends) == 0 {
		pickBackends = backup
	}
	if len(pickBackends) == 0 {
		fmt.Printf("[proxy] no backends available for %s\n", fs.id)
		return
	}

	bs := lb.Pick(pickBackends)

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
	go p.pipe(clientConn, backendConn, cs, &cs.bytesOut, &cs.samplingBytesOut, &cs.bs.bytesIn, &cs.fs.bytesIn, cfg.RateLimit, done)
	go p.pipe(backendConn, clientConn, cs, &cs.bytesIn, &cs.samplingBytesIn, &cs.bs.bytesOut, &cs.fs.bytesOut, cfg.RateLimit, done)

	<-done
	clientConn.Close()
	backendConn.Close()
	<-done

	p.stats.unregisterConn(connID)
	fmt.Printf("[proxy] %s: closed (in=%d out=%d)\n", connID, cs.bytesIn.Load(), cs.bytesOut.Load())
}

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
				expected := time.Duration(float64(byteCount) / float64(rateLimit) * float64(time.Second))
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
