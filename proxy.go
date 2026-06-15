package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Proxy struct {
	configs []FrontendConfig
	apiAddr string
	apiUser string
	apiPass string
	stats   *statsCollector
	lbCache map[string]loadBalancer
	connID  atomic.Int64
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

func (p *Proxy) listenFrontend(cfg FrontendConfig, fs *frontendStats) {
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[proxy] FATAL: listen %s: %v\n", cfg.ListenAddr, err)
		os.Exit(1)
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
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in handleConnection: %v\n%s", r, debug.Stack())
		}
		clientConn.Close()
	}()

	lb := p.lbCache[fs.id]
	bs := lb.Pick(fs.backends)
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

func (p *Proxy) pipe(dst io.Writer, src io.Reader, cs *connStats, connTotal, connSampling, backendTotal, frontendTotal *atomic.Int64, done chan bool) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[proxy] panic in pipe: %v\n%s", r, debug.Stack())
			done <- true
		}
	}()
	buf := make([]byte, 32*1024)
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
		}
		if err != nil {
			done <- true
			return
		}
	}
}
