SignalHub - Virtuals Source v1

Virtuals 新项目监听系统

版本
v1.0

目标
实现一个稳定运行的 Virtuals 项目监听器，能够自动发现新项目并记录项目状态变化，将所有变化输出为标准化事件，供 SignalHub 系统使用。

技术栈
Python
FastAPI
SQLite
Asyncio
APScheduler

一、项目背景

在 Virtuals 打新过程中，项目出现时间往往决定参与机会。

当前问题主要有三个。

人工监控成本高
需要反复刷新页面或信息源

发现速度慢
经常等到社交媒体传播后才注意到

信息结构化程度低
难以与自动化交易系统联动

因此需要构建一个自动化监听系统，用于：

持续监控 Virtuals 项目列表
第一时间发现新项目
检测项目状态变化
输出统一事件结构

该系统将作为 SignalHub 的第一号信号源。

二、系统目标

系统需要实现以下能力。

自动监听 Virtuals 项目列表

自动识别新项目

检测项目字段变化

记录事件日志

提供 API 查询

提供简单 Dashboard

第一版系统 不涉及交易逻辑，仅负责信号采集。

三、系统架构

整体结构如下

Virtuals Website / API
          │
          │ 轮询
          ▼
Virtuals Source
          │
          ▼
Project Parser
          │
          ▼
Entity Store
          │
          ▼
Event Generator
          │
          ▼
SignalHub Event Bus
          │
          ├─ Dashboard
          └─ API

系统核心由四个模块组成。

Source
负责抓取数据

Parser
负责解析项目信息

Entity Store
保存项目当前状态

Event Generator
生成标准事件

四、核心功能
1 Virtuals 项目监听

系统定期访问 Virtuals 项目列表页面或接口。

默认轮询周期

30 秒

每次轮询流程

1 获取项目列表
2 解析项目信息
3 与数据库实体对比
4 识别新增或变化
5 生成事件

2 项目识别

系统需要为每个项目生成唯一 ID。

优先顺序

project_id
slug
contract_address
url_hash

数据库中存在该 ID
说明项目已存在

数据库不存在
说明是新项目

3 项目字段

第一版仅保存最核心字段。

name
symbol
project_id
url
status
description
creator
created_time
last_seen
raw_hash

其中 raw_hash 用于检测变化。

raw_hash 生成方式

hash(name + status + description)

如果 hash 改变
说明项目内容发生变化。

4 事件生成

系统需要生成标准事件结构。

{
  "id": "evt_123456",
  "source": "virtuals",
  "type": "new_project_detected",
  "target": "project_id",
  "time": "2026-03-11T23:00:00Z",
  "payload": {
    "name": "project_name",
    "symbol": "symbol",
    "url": "project_url",
    "status": "detected"
  }
}

支持三种事件类型。

new_project_detected

发现新项目

project_updated

项目字段发生变化

project_status_changed

项目状态发生变化

五、数据库设计

数据库使用 SQLite。

需要三张表。

entities 表

保存当前项目状态。

entities

字段

id
project_id
name
symbol
url
status
description
creator
created_time
last_seen
raw_hash

说明

entities 表表示当前世界状态。

events 表

记录所有事件。

events

字段

id
event_id
source
type
target
time
payload

说明

events 表记录系统历史事件。

sources 表

保存监听源配置。

sources

字段

id
name
type
endpoint
interval
enabled
last_run

示例

virtuals_projects
http_polling
https://xxx/api/projects
30
true
六、轮询流程

完整轮询流程如下。

Scheduler Trigger
        │
        ▼
Fetch Project List
        │
        ▼
Parse Project Data
        │
        ▼
Check Entity Store
        │
 ┌──────┴───────┐
 │              │
New Project     Existing Project
 │              │
 ▼              ▼
Create Entity   Compare Hash
 │              │
 ▼              ▼
Create Event    Hash Changed ?
 │              │
 ▼              ▼
Save Event      Create Update Event
七、API 设计

FastAPI 提供查询接口。

获取事件列表

GET /events

参数

limit
offset
type
source

返回

[
  {
    "id": "...",
    "type": "...",
    "time": "...",
    "payload": {...}
  }
]

获取项目列表

GET /projects

返回

[
  {
    "name": "...",
    "symbol": "...",
    "status": "...",
    "last_seen": "..."
  }
]

获取项目详情

GET /projects/{project_id}

返回项目完整信息。

八、Dashboard

第一版 Dashboard 保持极简。

显示三块内容。

最近发现项目

最近事件

系统状态

界面示意

---------------------------------
SignalHub - Virtuals Monitor
---------------------------------

New Projects
---------------------------------
AI-Agent-XYZ
DeFi-Compute
Crypto-Auto

Recent Events
---------------------------------
23:05 New Project Detected
23:10 Project Updated
23:11 Status Changed

System Status
---------------------------------
Polling interval: 30s
Projects tracked: 38
Events recorded: 112

前端可以使用

HTML + JS
或简单 React

九、项目目录结构

推荐目录结构

signalhub
│
├─ app
│  ├─ main.py
│  ├─ config.py
│
│  ├─ sources
│  │   └─ virtuals_source.py
│
│  ├─ parsers
│  │   └─ virtuals_parser.py
│
│  ├─ processors
│  │   └─ event_processor.py
│
│  ├─ database
│  │   ├─ db.py
│  │   └─ models.py
│
│  ├─ api
│  │   └─ routes.py
│
│  └─ scheduler
│      └─ polling.py
│
└─ ui
    └─ dashboard
十、MVP 开发范围

第一版只实现以下能力。

监听 Virtuals 项目列表

识别新项目

检测字段变化

生成标准事件

提供 API 查询

提供基础 Dashboard

十一、未来扩展

未来版本可以增加以下能力。

GitHub 项目监听

RSS 监听

网页 diff 监听

Webhook 推送

规则引擎

策略系统

自动交易机器人

最终 SignalHub 将成为一个 通用信号采集中心。