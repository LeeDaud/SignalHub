# SignalHub - Virtuals Monitor

基于 FastAPI + SQLite + APScheduler 的 Virtuals 新项目监听 MVP。

## 能力

- 轮询 Virtuals 项目列表
- 识别新项目
- 检测项目字段与状态变化
- 生成标准事件并写入 SQLite
- 提供 FastAPI 查询接口
- 提供暗色系 Dashboard 与前端控制面板

## 默认数据源

当前默认直接接入 Virtuals 官方公开接口，并抓取待发射项目：

```text
https://api2.virtuals.io/api/virtuals
```

如果你只想离线跑样例数据，设置：

```bash
VIRTUALS_SAMPLE_MODE=true
```

## 安装

```bash
pip install -r requirements.txt
```

## 启动

方式一，自动打开浏览器中的 Dashboard：

```bash
python run_local.py
```

方式二，仅启动服务：

```bash
uvicorn signalhub.app.main:app --reload
```

如果使用方式二，请手动打开：

```text
http://127.0.0.1:8000/dashboard
```

## 关键配置

```bash
VIRTUALS_ENDPOINT=https://api2.virtuals.io/api/virtuals
VIRTUALS_APP_BASE_URL=https://app.virtuals.io
VIRTUALS_MODE=upcoming_launches
POLL_INTERVAL_SECONDS=30
VIRTUALS_PAGE_SIZE=100
VIRTUALS_PAGES=2
VIRTUALS_SORT=launchedAt:asc
VIRTUALS_SAMPLE_MODE=false
```

说明：

- `VIRTUALS_MODE=upcoming_launches` 会自动附加 `launchedAt > 当前时间` 和 `tokenAddress is null`
- 这对应的是“已预定发射时间、但尚未正式发射”的项目
- `VIRTUALS_PAGE_SIZE` 和 `VIRTUALS_PAGES` 控制每轮抓取的待发射项目范围
- 如果要回到最近创建项目模式，可以把 `VIRTUALS_MODE` 改成 `latest_created`

## 官方 API 示例

待发射项目列表：

```text
https://api2.virtuals.io/api/virtuals?sort=launchedAt:asc&filters[launchedAt][$gt]={now_iso}&filters[tokenAddress][$null]=true&pagination[page]=1&pagination[pageSize]=100
```

单项目详情：

```text
https://api2.virtuals.io/api/virtuals/{id}?populate=image,tags,framework,venturePartner.image,venturePartner.banner,genesis,vibesInfo
```

项目页面：

```text
https://app.virtuals.io/virtuals/{id}
```

对于 `UNDERGRAD` 或 `INITIALIZED` 且存在 `tokenAddress/preToken` 的项目，页面可能落在：

```text
https://app.virtuals.io/prototypes/{tokenAddress_or_preToken}
```

## 本地 API

- `GET /launches/upcoming`
- `GET /projects?upcoming_only=true&order_by=launch_time_asc`
- `GET /projects/{project_id}/contract`
- `GET /bot/feed/upcoming`
- `GET /bot/feed/events`
- `GET /bot/feed/snapshot`
- `GET /control/polling`
- `POST /control/polling/mode`
- `POST /control/polling/scan`

## 机器人 API 建议

- 机器人主入口优先使用 `GET /bot/feed/snapshot`
- 增量事件同步使用 `GET /bot/feed/events?limit=100&since={iso_time}`
- 候选项目池使用 `GET /bot/feed/upcoming?limit=50&within_hours=72`
- 单项目发射后补 CA 使用 `GET /projects/{project_id}/contract`

`/bot/feed/*` 会直接返回适合机器人消费的聚合 JSON，减少机器人端二次拼装字段的工作量。

## 前端控制

Dashboard 已内置以下控制能力，适合直接部署在服务器上远程操作：

- 自动模式：按 `POLL_INTERVAL_SECONDS` 定时轮询
- 切到自动模式：立即补跑一次扫描，然后进入定时轮询
- 手动模式：停止定时轮询，仅在点击按钮时执行一次扫描
- 执行一次扫描：立刻拉取待发射项目，并对已跟踪项目回刷详情以更新 CA
- 刷新面板：仅刷新页面数据，不触发后端扫描
- `Bot API Access` 面板：直接展示机器人接口 URL，支持预览 JSON、复制 URL、打开接口

其中，待发射列表和项目详情接口都会直接返回 `contract_address`；项目发射后，只要详情接口已给出 CA，下一轮扫描或手动扫描就会写回本地库并从 API 返回。
