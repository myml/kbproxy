package main

import (
	"bytes"
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
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	err := cmd.Run()
	wasHealthy := bs.healthy.Load()
	output := buf.String()
	if output != "" {
		fmt.Printf("[health] %s: check output:\n%s", bs.addr, output)
	}

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
