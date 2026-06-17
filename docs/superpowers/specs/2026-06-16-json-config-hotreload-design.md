# JSON 配置文件 + 热重载设计

## 目标

为 kbproxy 添加 JSON 配置文件支持，并在配置变更时无需重启即可生效（热重载）。

## 配置文件格式

复用现有 URL 格式（方案 C），仅用 JSON 做结构化组织：

```json
{
  "api": ":9090",
  "api_user": "admin",
  "api_pass": "secret",
  "frontends": [
    {
      "url": "tcp://:8080?lb=least_conn&rate_limit=10m",
      "backends": [
        "tcp://10.0.0.1:80?weight=3",
        "tcp://10.0.0.2:80?backup&check=/bin/check.sh&inter=10&check_timeout=3"
      ]
    },
    {
      "url": "tcp://:9091",
      "backends": ["tcp://10.0.0.3:3306"]
    }
  ]
}
```

JSON 结构体定义：

```go
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
```

解析时复用现有 `parseFrontendURL` / `parseBackendURL` 函数。

## 命令行交互

- 新增 `-config` 参数，指向 JSON 文件路径
- `-config` 与 `-frontend`/`-backend`/`-api` 等互斥，同时使用报错退出
- 仅 `-config` 模式支持热重载；命令行模式行为不变

## 热重载机制

### 变更检测

- 定时轮询文件 mtime，默认每 5 秒检查一次
- mtime 变化时重新读取并解析文件
- 零外部依赖，仅用 `os.Stat` + `time.Sleep`

### 前端变更

| 操作 | 行为 |
|------|------|
| 新增前端 | 启动新 listener goroutine |
| 移除前端 | 停止接受新连接，已有连接优雅等待关闭后再关闭 listener |
| 地址/策略变更 | 视为移除旧前端 + 新增新前端 |

**唯一标识**：前端 listen addr（如 `:8080`）作为唯一 key。

**优雅关闭**：每个前端 listener 增加 `draining` 标志和 `done channel`：
1. 设置 `draining = true`，`listener.Close()` 使 Accept 返回错误退出循环
2. 等待 `done channel`（所有已有连接关闭后关闭）
3. 为防止连接长时间不关闭，设置优雅等待超时（如 30 秒），超时后强制退出

### 后端变更

| 操作 | 行为 |
|------|------|
| 新增后端 | 加入后端池，启动健康检查 |
| 移除后端 | 标记 draining，不再被负载均衡选中，已有连接继续服务直到关闭 |
| 参数变更（weight/backup/check 等） | 更新配置，健康检查参数变更时重启检查 |

**唯一标识**：后端 addr（如 `10.0.0.1:80`）作为唯一 key。

### API 配置变更

api_user / api_pass 变更时即时更新 Proxy 字段即可（已有连接不受影响，新请求使用新凭据）。

api 监听地址变更不支持热重载（需重启服务），配置变更时若 api 地址不同则打印警告并忽略。

## 核心改动

### 1. 配置加载（main.go）

- 新增 `-config` flag
- 新增 `loadConfigFile(path string) (*ConfigFile, error)` 函数
- 新增 `configToProxyConfigs(cf *ConfigFile) ([]FrontendConfig, string, string, string, error)` 函数
- 互斥检查逻辑

### 2. 热重载 goroutine（新文件 reload.go）

- `watchConfigFile(path string, interval time.Duration, onChange func(*ConfigFile))`
- 轮询 mtime，变化时解析并调用回调
- 解析失败时打印错误并跳过（不中断服务）

### 3. Proxy 动态管理（proxy.go）

- `Proxy` 增加 `map[string]*frontendListener` 跟踪活跃前端
- `frontendListener` 结构体：`listener`、`draining` 标志、`done channel`、`activeConns` 计数
- 新增 `ReloadConfig(configs []FrontendConfig)` 方法：diff 新旧配置，执行增删改
- 新增 `addFrontend(cfg FrontendConfig)` / `removeFrontend(addr string)` 方法
- 后端 `backendStats` 增加 `draining` 标志
- 负载均衡 Pick 时跳过 draining 后端

### 4. 健康检查动态管理（healthcheck.go）

- 支持停止单个后端的健康检查
- 支持重启健康检查（参数变更时）

### 5. README 更新

- 补充 `-config` 用法
- 补充热重载说明
- 补充配置文件示例

## 错误处理

- 配置文件读取/解析失败：打印错误，保持当前配置继续运行
- 配置文件不存在：启动时报错退出
- 热重载时解析失败：打印错误，跳过本次变更
- 优雅关闭超时：30 秒后强制退出相关 goroutine
