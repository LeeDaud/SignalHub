# Virtuals API 接入建议

以下方式基于 Virtuals 官方前端实际使用的公开接口整理。

## 推荐抓取策略

1. 用列表接口轮询待发射项目，按 `launchedAt:asc` 排序。
2. 过滤条件使用 `launchedAt > now` 且 `tokenAddress is null`。
3. 用列表结果中的 `id` 作为主键，优先识别待发射项目。
4. 对新项目或重点项目，再补一次详情接口获取更完整字段。
5. 下游系统优先消费本项目输出的 `/events`，不要每个消费者都直接打 Virtuals。

## 推荐接口

项目列表：

```text
GET https://api2.virtuals.io/api/virtuals?sort=launchedAt:asc&filters[launchedAt][$gt]={now_iso}&filters[tokenAddress][$null]=true&pagination[page]=1&pagination[pageSize]=100
```

项目详情：

```text
GET https://api2.virtuals.io/api/virtuals/{id}?populate=image,tags,framework,venturePartner.image,venturePartner.banner,genesis,vibesInfo
```

## 建议的筛选参数

按待发射时间：

```text
filters[launchedAt][$gt]={now_iso}
filters[tokenAddress][$null]=true
```

按链：

```text
filters[chain][$eq]=BASE
```

按状态：

```text
filters[status][$in][0]=UNDERGRAD
filters[status][$in][1]=INITIALIZED
filters[status][$in][2]=ACTIVE
```

按分类：

```text
filters[category][$in][0]=ACP_LAUNCH
```

## 对接建议

- 做“新项目发现”时，优先轮询最近 1 到 3 页。
- 做“发射时间变更跟踪”或“CA 回填”时，建议对最近已跟踪项目追加详情拉取。
- 如果后续要做高频监控，建议在你自己的服务内做事件归档和去重，不要把 Virtuals 官方 API 当事件总线直接消费。

## 本地服务建议 API

前端面板或下游系统建议优先对接本项目暴露的本地 API，而不是各自直连 Virtuals：

- `GET /launches/upcoming`
  返回当前待发射项目列表，包含 `display_title`、`launch_time`、`contract_address`
- `GET /projects/{project_id}`
  返回单项目完整信息
- `GET /projects/{project_id}/contract`
  专门读取项目 CA、状态和发射时间，适合交易或告警侧轮询
- `GET /events`
  读取新增项目、状态变化、字段更新事件
- `GET /bot/feed/upcoming`
  机器人友好的待发射项目聚合接口，支持 `within_hours` 和 `contract_ready_only`
- `GET /bot/feed/events`
  机器人友好的事件流接口，支持 `since` 和 `event_types`
- `GET /bot/feed/snapshot`
  一次返回轮询状态、待发射项目和最新事件，适合作为机器人主轮询入口
- `GET /control/polling`
  读取当前轮询模式、最近执行结果、错误信息
- `POST /control/polling/mode`
  在前端切换 `auto` / `manual`，切到 `auto` 时会立即补跑一次扫描
- `POST /control/polling/scan`
  手动触发一次扫描

## 机器人调用建议

- 主循环：`GET /bot/feed/snapshot?project_limit=20&event_limit=20&within_hours=72`
- 事件增量：`GET /bot/feed/events?limit=100&since={last_event_time}`
- CA 回查：`GET /projects/{project_id}/contract`
- 前端面板中的 `Bot API Access` 会直接展示这些接口的完整 URL，并支持复制和 JSON 预览
