# TCP Proxy (kbproxy) 设计文档

## 概述

一个基于 Go 的 TCP 代理服务，支持单端口转发到多台后端服务器，提供 Web 界面实时查看每层（前端、后端、连接）的流量速率。

## 架构

```
┌──────────────────────────────────────────────────┐
│                    kbproxy                        │
│                                                    │
│  ┌──────────┐   ┌──────────────────────────────┐  │
│  │ TCP      │──▶│  Proxy Handler                 │  │
│  │ Listener │   │  ┌─────────┐  ┌─────────┐    │  │
│  │ :8080    │   │  │ Conn A  │  │ Conn B  │    │  │
│  └──────────┘   │  │  ──▶ B1 │  │  ──▶ B2 │    │  │
│                  │  │  ◀── B1 │  │  ◀── B2 │    │  │
│                  │  └─────────┘  └─────────┘    │  │
│                  └──────────────────────────────┘  │
│                                    │                │
│  ┌──────────┐   ┌──────────────────▼───────────┐  │
│  │ HTTP API │◀──│  Stats Collector               │  │
│  │ :9090    │   │  - Per frontend rate           │  │
│  └──────────┘   │  - Per backend rate            │  │
│                  │  - Per connection rate         │  │
│                  └──────────────────────────────┘  │
└────────────────────────────────────────────────────┘
```

## 组件设计

### 1. proxy.go - TCP 监听与连接转发

- `Frontend`：一个 TCP listener，绑定多个 Backend
- `Backend`：上游服务器地址 (host:port)
- 当新连接到达时，根据负载均衡算法选择一个 Backend，建立到后端的 TCP 连接，然后双向转发数据
- 双向转发在两个 goroutine 中完成（client→backend, backend→client），每个方向都经过 byte counter 统计

### 2. lb.go - 负载均衡策略

两种策略，可通过配置文件选择：

- **LeastConnections**：选择当前活跃连接数最少的 Backend
- **LeastBandwidth**：选择当前出流量速率（bytes_out_rate）最低的 Backend

### 3. stats.go - 三层流量统计

统计分为三层，每层都记录累计字节数和实时速率：

| 层级 | 范围 | 关键字段 |
|------|------|---------|
| Frontend | 整个 listener | 所有连接聚合的入/出流量、速率 |
| Backend | 单个后端服务器 | 发往该后端的入/出流量聚合、速率 |
| Connection | 单个 TCP 连接 | 该连接的入/出流量、速率、状态 |

速率计算使用滑动窗口（Sliding Window）：每个统计对象维护近 10 秒的每秒采样点环形缓冲区，每秒计算一次平均速率。

### 4. api.go - HTTP API 与 Web UI

监听独立端口（默认 9090），提供以下端点：

| 路径 | 方法 | 说明 |
|------|------|------|
| `/` | GET | 内嵌 HTML 页面 |
| `/api/connections` | GET | 所有活跃连接，含所属前端/后端及速率，前端 JS 负责分组聚合 |

API 响应示例：

```json
GET /api/connections
[
  {
    "id": "c-1",
    "frontend": ":8080",
    "backend": "192.168.1.1:9001",
    "bytes_in": 1048576,
    "bytes_out": 2097152,
    "rate_in": 512000,
    "rate_out": 384000,
    "created_at": "14:30:05"
  }
]
```

前端 JS 按 `frontend` 分组汇总得到前端统计，再按 `backend` 分组汇总得到后端统计。

### 5. 前端页面

内嵌 HTML + CSS + JS：
- 表格展示 Frontend 列表及聚合统计
- 点击展开查看每个 Backend 和活跃连接详情
- 使用 `setInterval` 每 1-2 秒轮询 API 刷新数据
- 实时速率用数字或简单的柱状条展示

## 配置与启动

Web UI 默认无密码访问；可通过 `-api-user` 和 `-api-pass` 启用 Basic Auth。

Frontend 和 Backend 使用 URL 格式 `tcp://host:port`，Frontend 可通过查询参数 `?lb=least_conn|least_bandwidth` 指定负载均衡策略。

通过命令行参数或环境变量配置：

```sh
# 启动一个 frontend 转发到两个 backend
./kbproxy \
  -frontend "tcp://:8080?lb=least_conn" \
  -backend "tcp://192.168.1.1:9001,tcp://192.168.1.2:9001" \
  -api ":9090"

# 多个 frontend（可多次指定）
./kbproxy \
  -frontend "tcp://:8080?lb=least_bandwidth" \
  -backend "tcp://10.0.0.1:80,tcp://10.0.0.2:80" \
  -frontend "tcp://:8443" \
  -backend "tcp://10.0.1.1:443,tcp://10.0.1.2:443" \
  -api ":9090"

# 带 Basic Auth
./kbproxy \
  -frontend "tcp://:8080" \
  -backend "tcp://10.0.0.1:80" \
  -api ":9090" \
  -api-user "admin" \
  -api-pass "secret123"
```

## 项目结构

```
kbproxy/
├── main.go          # 入口：解析参数、启动各组件
├── proxy.go         # Frontend/Backend 定义，连接转发逻辑
├── lb.go            # 负载均衡算法
├── stats.go         # 三层统计（Frontend/Backend/Connection）
├── api.go           # HTTP API 服务
└── docs/
    └── specs/
        └── 2025-06-15-tcp-proxy-design.md
```

## 技术选型

- Go 标准库 `net` 处理 TCP
- Go 标准库 `net/http` 提供 API
- 内嵌 HTML 使用 Go 1.16+ `embed` 包
- 无第三方依赖

## YAGNI 排除项

- 无需 TLS 终止（纯透传 TCP 代理）
- 无需持久化存储统计（重启丢失可接受）

- 无需健康检查（后续按需添加）
- 无需 gRPC / protobuf
