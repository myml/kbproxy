# Frontend Rate Limiting Design

## Overview

Add per-connection rate limiting to kbproxy, configured at the frontend level via URL parameters. Rate limiting applies to each individual TCP connection independently using a simple sleep-based approach.

## Configuration

**URL parameter**: `rate_limit` on frontend URL

```
kbproxy -frontend tcp://:8080?rate_limit=10m -backend tcp://10.0.0.1:80
```

**Unit format**: numeric value with optional suffix
- `500k` = 500 KB/s
- `10m` = 10 MB/s
- `1g` = 1 GB/s
- `1048576` = 1048576 B/s (pure number = bytes/s)
- Omit or `0` = no rate limiting

## Algorithm: Sleep-based Rate Limiting

In the `pipe()` function, after each write operation:

1. Calculate expected time: `expected = bytesWritten / rateLimit`
2. Measure elapsed time since the write started
3. If `expected > elapsed`, sleep for `expected - elapsed`
4. If `rateLimit <= 0`, skip all calculations (no limiting)

Each direction of a connection (client->backend, backend->client) is limited independently with the same rate limit value. Different connections are independent of each other.

### Characteristics
- Simple implementation, minimal code changes
- Allows short bursts (one buffer's worth of data may be sent instantly before sleep)
- Sufficient precision for proxy use cases

## Code Changes

### `main.go`

- Add `RateLimit int64` field to `FrontendConfig` struct
- Parse `rate_limit` query parameter from frontend URL
- Add `parseRateLimit(s string) (int64, error)` function supporting `k`/`m`/`g` suffixes

### `proxy.go`

- Pass `rateLimit` from `FrontendConfig` through to `handleConnection`
- `pipe()` signature: add `rateLimit int64` parameter
- Implement sleep-based throttling in `pipe()`:
  - Track start time before each write cycle
  - After write, compute sleep duration and `time.Sleep` if needed
  - Skip when `rateLimit <= 0`

### `stats.go`

- Add `RateLimit int64` field to `frontendStats` struct
- Populate `RateLimit` from frontend config in stats collection
- Include `rate_limit` in API JSON response for frontends

## What Does NOT Change

- Dashboard: no UI changes (no display of rate limit info)
- Backend URL parameters: no new parameters
- Existing behavior: when `rate_limit` is not specified, behavior is identical to current
- Load balancing logic: unaffected
- Health checks: unaffected
