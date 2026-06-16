package main

import (
	"context"
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
	defer sw.mu.Unlock()
	sw.buf[sw.pos] = sample
	sw.pos++
	if sw.pos == windowSize {
		sw.pos = 0
		sw.full = true
	}
}

func (sw *slidingWindow) rate() float64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	var sum int64
	var count int
	if sw.full {
		count = windowSize
		for _, v := range sw.buf {
			sum += v
		}
	} else if sw.pos > 0 {
		count = sw.pos
		for i := 0; i < sw.pos; i++ {
			sum += sw.buf[i]
		}
	}
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

type connStats struct {
	id              string
	bytesIn         atomic.Int64 // updated externally via atomic.AddInt64 in proxy pipe goroutines
	bytesOut        atomic.Int64 // updated externally via atomic.AddInt64 in proxy pipe goroutines
	samplingBytesIn  atomic.Int64
	samplingBytesOut atomic.Int64
	rateIn          *slidingWindow
	rateOut         *slidingWindow
	createdAt       time.Time
	closed          atomic.Bool
	bs              *backendStats
	fs              *frontendStats
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
	addr           string
	weight         int
	backup         bool
	bytesIn        atomic.Int64
	bytesOut       atomic.Int64
	rateIn         *slidingWindow
	rateOut        *slidingWindow
	activeConns    atomic.Int64
	totalConns     atomic.Int64
	peakConns      atomic.Int64
	peakRateIn     atomic.Int64
	peakRateOut    atomic.Int64
	healthy        atomic.Bool
	draining       atomic.Bool
	lastCheckTime  atomic.Int64
	checkInterval  atomic.Int64
	checkTotal     atomic.Int64
	checkSuccess   atomic.Int64
}

func newBackendStats(addr string, weight int, backup bool) *backendStats {
	bs := &backendStats{
		addr:    addr,
		weight:  weight,
		backup:  backup,
		rateIn:  newSlidingWindow(),
		rateOut: newSlidingWindow(),
	}
	bs.healthy.Store(true)
	return bs
}

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

func newFrontendStats(id, listenAddr string) *frontendStats {
	return &frontendStats{
		id:         id,
		listenAddr: listenAddr,
		rateIn:     newSlidingWindow(),
		rateOut:    newSlidingWindow(),
	}
}

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

func (sc *statsCollector) unregisterFrontend(id string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.frontends, id)
}

func (sc *statsCollector) updateFrontendBackends(fs *frontendStats, backendConfigs []BackendConfig, cancelFuncs map[string]context.CancelFunc) map[string]context.CancelFunc {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	newCancelFuncs := make(map[string]context.CancelFunc)

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

func (sc *statsCollector) registerConn(id string, fs *frontendStats, bs *backendStats) *connStats {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	cs := newConnStats(id)
	cs.fs = fs
	cs.bs = bs
	sc.conns[id] = cs
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
	if cs.bs != nil {
		cs.bs.activeConns.Add(-1)
	}
	if cs.fs != nil {
		cs.fs.activeConns.Add(-1)
	}
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

// Lock ordering: sc.mu acquired first, then sw.mu (via sw.add()). No reverse path exists.
func (sc *statsCollector) sampleAll() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	beSamples := make(map[*backendStats][2]int64)
	fsSamples := make(map[*frontendStats][2]int64)

	for _, cs := range sc.conns {
		if cs.closed.Load() {
			continue
		}
		sampleIn := cs.samplingBytesIn.Swap(0)
		sampleOut := cs.samplingBytesOut.Swap(0)
		cs.rateIn.add(sampleIn)
		cs.rateOut.add(sampleOut)

		if cs.bs != nil {
			s := beSamples[cs.bs]
			s[0] += sampleIn
			s[1] += sampleOut
			beSamples[cs.bs] = s
		}
		if cs.fs != nil {
			s := fsSamples[cs.fs]
			s[0] += sampleIn
			s[1] += sampleOut
			fsSamples[cs.fs] = s
		}
	}

	for _, fs := range sc.frontends {
		var totalIn, totalOut int64
		for _, bs := range fs.backends {
			if s, ok := beSamples[bs]; ok {
				bs.rateIn.add(s[0])
				bs.rateOut.add(s[1])
				totalIn += s[0]
				totalOut += s[1]
			}
			if r := int64(bs.rateIn.rate()); r > bs.peakRateIn.Load() {
				bs.peakRateIn.Store(r)
			}
			if r := int64(bs.rateOut.rate()); r > bs.peakRateOut.Load() {
				bs.peakRateOut.Store(r)
			}
			if n := bs.activeConns.Load(); n > bs.peakConns.Load() {
				bs.peakConns.Store(n)
			}
		}
		if s, ok := fsSamples[fs]; ok {
			fs.rateIn.add(s[0])
			fs.rateOut.add(s[1])
		}
		if r := int64(fs.rateIn.rate()); r > fs.peakRateIn.Load() {
			fs.peakRateIn.Store(r)
		}
		if r := int64(fs.rateOut.rate()); r > fs.peakRateOut.Load() {
			fs.peakRateOut.Store(r)
		}
		if n := fs.activeConns.Load(); n > fs.peakConns.Load() {
			fs.peakConns.Store(n)
		}
	}
}
