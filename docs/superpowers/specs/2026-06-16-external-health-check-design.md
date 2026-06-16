# 外部健康检查设计

## 概述

为 kbproxy 添加类似 HAProxy 的外部健康检查功能。每个后端可配置一个检查脚本，脚本周期性执行，执行时通过环境变量接收后端的 IP 和端口。健康检查失败的后端将被排除在负载均衡之外。

## 命令行参数

后端 URL 扩展三个查询参数：

```
tcp://10.0.0.1:80?weight=2&check=/usr/local/bin/check.sh&inter=30&check_timeout=5
```

| 参数 | 是否必填 | 默认值 | 说明 |
|------|----------|--------|------|
| `check` | 否 | 无 | 健康检查脚本路径。不设置则不进行健康检查。 |
| `inter` | 否 | 60 | 检查间隔，单位秒。 |
| `check_timeout` | 否 | 5 | 脚本执行超时，单位秒。 |

## 数据结构变更

### BackendConfig (main.go)

```go
type BackendConfig struct {
    Addr          string
    Weight        int
    CheckScript   string
    CheckInterval time.Duration
    CheckTimeout  time.Duration
}
```

`parseBackendURL` 解析三个新增查询参数。默认值：`inter=60s`，`check_timeout=5s`。

### backendStats (stats.go)

```go
type backendStats struct {
    // 已有字段...
    healthy atomic.Bool
}
```

`healthy` 默认为 `true`。检查失败时设为 `false`，检查成功时恢复为 `true`。

## 健康检查流程

1. 在 `registerFrontend` 中，为每个配置了 `CheckScript` 的后端启动一个健康检查 goroutine。
2. goroutine 使用 `time.Ticker` 按 `CheckInterval` 间隔循环执行。
3. 每次触发：
   - 使用 `net.SplitHostPort` 将 `addr` 拆分为 host 和 port。
   - 设置环境变量：`KBPROXY_BACKEND_HOST`（IP/主机名）、`KBPROXY_BACKEND_PORT`（端口）。
   - 使用 `exec.CommandContext` 执行脚本，超时时间为 `CheckTimeout`。
   - 退出码 0 → `healthy.Store(true)`。
   - 退出码非零或超时 → `healthy.Store(false)`。
   - 状态变化时输出日志。
4. goroutine 随进程生命周期运行，无需显式关闭机制。

## 负载均衡集成

在 `handleConnection` 中，调用 `lb.Pick()` 之前：

1. 过滤 `fs.backends`，仅保留 `healthy.Load() == true` 的后端。
2. 将过滤后的列表传给 `lb.Pick()`。
3. 若所有后端都不健康（过滤后列表为空），回退使用完整的未过滤列表，防止全部后端检查失败时完全不可用。

## API 与监控

### /api/stats

`backendStat` 新增 `Healthy bool` 字段：

```go
type backendStat struct {
    // 已有字段...
    Healthy bool `json:"healthy"`
}
```

### index.html

前端页面在每个后端地址旁显示健康状态标识（如彩色圆点或文字标签）。

## 错误处理

- 检查脚本路径不存在或不可执行时，记录错误日志并标记后端为不健康。
- 脚本执行超时视为检查失败（不健康）。
- 不捕获脚本输出，仅以退出码判断健康状态。

## 使用示例

```bash
kbproxy \
  -frontend "tcp://:8080?lb=least_conn" \
  -backend "tcp://10.0.0.1:3306?weight=1&check=/usr/local/bin/check_mysql.sh&inter=30&check_timeout=3,tcp://10.0.0.2:3306?weight=2&check=/usr/local/bin/check_mysql.sh&inter=30&check_timeout=3"
```

示例检查脚本：

```bash
#!/bin/bash
mysqladmin ping -h "$KBPROXY_BACKEND_HOST" -P "$KBPROXY_BACKEND_PORT" --silent
```
