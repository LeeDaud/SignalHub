# SignalHub - Virtuals Monitor

基于 `FastAPI + SQLite + APScheduler + Chainstack` 的 Virtuals 项目监听服务。  
当前版本重点解决两件事：

- 监听 Virtuals 官方待发射项目
- 自动识别 `TA(Token Address)` 与 `PA(Pool / Internal Market Address)`，并通过 API 与前端输出

## 功能概览

- 轮询 Virtuals 官方项目列表与详情
- 跟踪待发射项目、生命周期、状态变化和事件
- 通过 Chainstack 在 Base 链上回补首批 ERC20 `Transfer` 记录
- 按 `0x0000... -> 中间地址 -> PA` 规则自动识别内盘主池地址
- 将 `TA / PA` 回写数据库、前端面板和机器人 API
- 提供统一机器人接口 `GET /bot/feed/unified`
- 提供本地 Dashboard 控制面板

## 目录说明

- `signalhub/app/`：后端主代码
- `signalhub/ui/dashboard/index.html`：前端面板
- `.env`：本地环境配置
- `signalhub.db`：本地 SQLite 数据库
- `exports/token-pools.json`：导出的池地址 JSON
- `run_local.py`：本地启动入口
- `start_service.ps1 / stop_service.ps1 / restart_service.ps1`：后台服务脚本

## 运行要求

- Python `3.11+`
- Windows PowerShell
- 可访问 Virtuals API
- 如需链上自动识别 `PA`，需要可用的 Chainstack Base RPC

安装依赖：

```bash
pip install -r requirements.txt
```

## 本地部署

### 1. 检查 `.env`

项目根目录已内置一个 `.env` 文件，默认路径：

```text
.env
```

启动前至少确认这些变量：

```bash
APP_NAME="SignalHub - Virtuals Monitor"
SIGNALHUB_DB_PATH=signalhub.db
DASHBOARD_PATH=signalhub/ui/dashboard/index.html
TOKEN_POOL_EXPORT_PATH=exports/token-pools.json

SOURCE_ENABLED=true
POLL_INTERVAL_SECONDS=30
REQUEST_TIMEOUT_SECONDS=15

VIRTUALS_ENDPOINT=https://api2.virtuals.io/api/virtuals
VIRTUALS_APP_BASE_URL=https://app.virtuals.io
VIRTUALS_MODE=upcoming_launches
VIRTUALS_SAMPLE_MODE=false

CHAINSTACK_BASE_HTTPS_URL=https://base-mainnet.core.chainstack.com/<your-project-id>
CHAINSTACK_BASE_WSS_URL=wss://base-mainnet.core.chainstack.com/<your-project-id>
CHAINSTACK_SUBSCRIPTION_ENABLED=true
```

### 2. 选择运行模式

#### 模式 A：真实线上模式

用于真实 Virtuals + Base 链监听：

```bash
VIRTUALS_SAMPLE_MODE=false
CHAINSTACK_BASE_HTTPS_URL=https://base-mainnet.core.chainstack.com/<your-project-id>
CHAINSTACK_BASE_WSS_URL=wss://base-mainnet.core.chainstack.com/<your-project-id>
```

说明：

- `CHAINSTACK_BASE_HTTPS_URL`：历史查询、回补、trace 使用
- `CHAINSTACK_BASE_WSS_URL`：常驻订阅日志使用
- 没有这两个地址，项目仍可跑 Virtuals 数据抓取，但不会自动识别 `PA`

#### 模式 B：样例数据模式

如果只是本地看 UI 或调试，不依赖真实接口：

```bash
VIRTUALS_SAMPLE_MODE=true
```

此模式下会直接读取：

```text
sample_data/virtuals_projects.json
```

### 3. 启动项目

推荐使用：

```bash
python run_local.py
```

它会：

- 启动 FastAPI 服务
- 自动打开浏览器
- 默认访问：

```text
http://127.0.0.1:8000/dashboard
```

如果只想启动服务：

```bash
uvicorn signalhub.app.main:app --reload
```

## 服务脚本

如果项目部署在服务器或需要后台运行，使用这些脚本：

```powershell
.\start_service.ps1 -Port 8000
.\stop_service.ps1 -Port 8000
.\restart_service.ps1 -Port 8000
```

也提供 `.cmd` 包装：

```cmd
start_service.cmd -Port 8000
stop_service.cmd -Port 8000
restart_service.cmd -Port 8000
```

## `.env` 关键变量说明

### 基础运行

- `APP_NAME`：应用名称
- `SIGNALHUB_DB_PATH`：SQLite 数据库路径
- `DASHBOARD_PATH`：Dashboard HTML 路径
- `TOKEN_POOL_EXPORT_PATH`：导出 JSON 路径
- `POLL_INTERVAL_SECONDS`：轮询间隔
- `REQUEST_TIMEOUT_SECONDS`：HTTP 请求超时

### Virtuals 数据源

- `VIRTUALS_ENDPOINT`：Virtuals API 地址
- `VIRTUALS_APP_BASE_URL`：Virtuals 页面基地址
- `VIRTUALS_MODE`：当前建议使用 `upcoming_launches`
- `VIRTUALS_PAGE_SIZE`：单页抓取数量
- `VIRTUALS_PAGES`：每轮抓取页数
- `VIRTUALS_DETAIL_REFRESH_LIMIT`：每轮详情补刷数量
- `VIRTUALS_SORT`：排序方式，待发射模式建议 `launchedAt:asc`
- `VIRTUALS_SAMPLE_MODE`：是否使用样例数据

### Chainstack / 链上追踪

- `CHAINSTACK_BASE_HTTPS_URL`：Base HTTPS RPC
- `CHAINSTACK_BASE_WSS_URL`：Base WSS RPC
- `CHAINSTACK_SUBSCRIPTION_ENABLED`：是否开启常驻订阅
- `CHAINSTACK_SUBSCRIPTION_REFRESH_SECONDS`：订阅刷新周期
- `CHAINSTACK_TRACE_BACKFILL_ENABLED`：是否启用历史回补
- `CHAINSTACK_TRACE_BACKFILL_BATCH_SIZE`：每轮回补项目数
- `CHAINSTACK_TRACE_BACKFILL_COOLDOWN_SECONDS`：单项目回补冷却时间
- `CHAINSTACK_LOG_CHUNK_SIZE`：链上日志扫描块大小
- `CHAINSTACK_EARLIEST_SCAN_WINDOW_BLOCKS`：首批日志扫描窗口
- `CHAINSTACK_EARLIEST_BATCH_SIZE`：首批记录保留数量
- `CHAINSTACK_PATTERN_LOG_LIMIT`：用于模式识别的最早日志条数

### 兼容字段

以下字段当前不是主链路必需，仅作兼容保留：

- `BASESCAN_API_KEY`
- `ETHERSCAN_API_KEY`
- `ETHERSCAN_BASE_API_URL`
- `BASE_CHAIN_ID`

## 本地访问地址

启动后常用入口：

- Dashboard：
  [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)
- 系统状态：
  [http://127.0.0.1:8000/system/status](http://127.0.0.1:8000/system/status)
- 项目列表：
  [http://127.0.0.1:8000/projects?upcoming_only=true&order_by=launch_time_asc](http://127.0.0.1:8000/projects?upcoming_only=true&order_by=launch_time_asc)

## 机器人接口

### 推荐主入口

统一接口：

```text
GET /bot/feed/unified
```

示例：

```text
http://127.0.0.1:8000/bot/feed/unified?project_limit=100&event_limit=100&internal_market_limit=200&token_pool_limit=2000&within_hours=168
```

该接口包含：

- `launches`
- `internal_markets`
- `token_pools`
- `events`
- `source / control / summary`

### 其他接口

保留的拆分接口：

- `GET /bot/feed/upcoming`
- `GET /bot/feed/internal-markets`
- `GET /bot/feed/token-pools`
- `GET /bot/feed/events`
- `GET /bot/feed/snapshot`

### 导出 JSON

可直接预览的 JSON：

```text
GET /exports/token-pools.json
```

说明：

- 现在会直接返回 `application/json`
- 浏览器打开时可直接预览，不再强制下载

## 当前识别逻辑

对于 Virtuals 项目：

1. 从 Virtuals 项目 URL 中提取 `TA`
2. 在 Base 链上查询该 `TA` 最早命中的 ERC20 `Transfer`
3. 锁定首个命中区块
4. 提取该区块内的首批交易记录
5. 优先匹配：

```text
0x0000... -> 中间地址 -> PA
```

6. 命中后自动回写：

- `entities.internal_market_address`
- `project_launch_traces`
- `exports/token-pools.json`
- Dashboard 项目卡片
- 机器人统一 API

## 常见问题

### 1. 运行时报端口占用

如果 `8000` 已被占用：

```powershell
.\restart_service.ps1 -Port 8000
```

或者：

```powershell
.\stop_service.ps1 -Port 8000
python run_local.py
```

### 2. 前端能看到项目，但没有 `PA`

先检查：

- `.env` 中的 `CHAINSTACK_BASE_HTTPS_URL`
- `.env` 中的 `CHAINSTACK_BASE_WSS_URL`
- `GET /system/status` 中 `chainstack_subscription.connected` 是否为 `true`

如果项目刚被抓到，等待一个轮询周期即可；系统会自动对待发射项目做回补。

### 3. Chainstack 返回 403

说明当前 RPC 计划不支持某些历史或 trace 能力。  
当前代码已对这类情况做降级处理，但若要更完整的链上追踪能力，仍建议使用支持对应方法的节点计划。

### 4. 只想本地看页面，不想接真实链上

直接把：

```bash
VIRTUALS_SAMPLE_MODE=true
```

即可。

## 验证本地部署是否成功

启动后至少检查这 4 个点：

1. 打开 Dashboard：
   [http://127.0.0.1:8000/dashboard](http://127.0.0.1:8000/dashboard)
2. 打开系统状态：
   [http://127.0.0.1:8000/system/status](http://127.0.0.1:8000/system/status)
3. 打开统一机器人接口：
   [http://127.0.0.1:8000/bot/feed/unified](http://127.0.0.1:8000/bot/feed/unified)
4. 打开池地址导出：
   [http://127.0.0.1:8000/exports/token-pools.json](http://127.0.0.1:8000/exports/token-pools.json)

如果以上都能正常返回，本地部署基本完成。
