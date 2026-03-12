Virtuals Source v2

Virtuals 项目情报系统

版本
v2.0

目标
在 v1 的基础上增加项目生命周期追踪、页面变化 diff、项目评分系统以及链上地址关联能力，使系统能够对 Virtuals 项目进行持续分析与情报整理。

技术栈
Python
FastAPI
SQLite 或 PostgreSQL
Asyncio
APScheduler

一、项目背景

Virtuals 打新过程中，项目质量和启动节奏差异极大。

当前系统 v1 只能解决一个问题：

发现新项目提供机器人接口。

但在实际参与过程中，还需要回答更多问题：

项目是否在推进
项目内容是否发生变化
团队信息是否完整
项目是否可能进入 Launch 阶段
项目背后地址是否可信

因此需要升级系统，使其具备 项目持续观察能力。

二、系统目标

v2 系统需要实现以下能力：

记录项目生命周期

检测项目页面变化并生成 diff

生成项目评分

识别项目链上地址

提供观察列表

提供项目分析界面

系统仍然不包含交易功能，仅提供情报与数据支持。

三、系统架构

系统架构如下：

Virtuals Source
        │
        ▼
Project Parser
        │
        ▼
Project Database
        │
 ┌──────┴────────┐
 │               │
Lifecycle Engine Diff Engine
 │               │
 ▼               ▼
Score Engine  Address Analyzer
        │
        ▼
API + Dashboard

系统新增四个核心模块：

Lifecycle Engine
Diff Engine
Score Engine
Address Analyzer

四、核心功能
1 项目生命周期追踪

每个 Virtuals 项目在系统中拥有生命周期状态。

建议定义以下阶段：

detected
info_updated
launch_announced
launch_open
launch_live
launch_closed
token_trading
inactive

系统根据页面字段变化自动判断阶段。

例如：

首次发现项目
stage = detected

项目新增 launch_time
stage = launch_announced

页面出现 launch_open
stage = launch_open

阶段变化将生成事件。

2 页面变化检测

v2 系统需要记录页面内容变化。

系统将保存项目页面结构快照。

保存内容包括：

description
team
links
tokenomics
launch_time

每次抓取页面时进行 diff。

diff 示例：

description updated
launch_time added
token_supply changed
team section added

生成事件：

project_content_updated

数据库保存变化记录。

3 项目评分系统

系统为每个项目生成基础评分。

评分规则示例：

团队信息存在        +1
项目描述完整        +1
Tokenomics 信息存在 +1
外部链接存在        +1
GitHub 存在         +1

评分范围：

0 - 5

评分字段：

project_score
risk_level

risk_level 示例：

low
medium
high

评分将在 Dashboard 中展示。

4 链上地址关联

系统尝试识别项目相关链上地址。

可能来源：

creator wallet
token contract
liquidity pool
treasury wallet

识别方式：

页面解析
接口数据
链上事件分析

地址数据保存至数据库：

project_addresses

字段：

address
address_type
chain

address_type 示例：

creator
token_contract
liquidity_pool
treasury
5 项目观察列表

用户可以将项目加入观察列表。

观察列表功能：

持续追踪变化

单独通知

优先展示

数据库字段：

watchlist

类型：

boolean
6 项目分析页面

Dashboard 新增项目详情页。

展示内容包括：

项目基本信息

生命周期阶段

评分

页面变化记录

链上地址

事件时间线

页面示意：

Project: AI-Agent-X

Stage: launch_announced
Score: 4
Risk: medium

Recent Changes
--------------------------------
launch_time added
description updated

Addresses
--------------------------------
creator: 0x...
token: 0x...

Events
--------------------------------
22:01 project_detected
22:15 description_updated
22:40 launch_announced
五、数据库设计

v2 数据库新增两张表。

project_snapshots

保存页面快照。

字段：

id
project_id
snapshot_time
description
team
links
tokenomics
launch_time
raw_data
project_changes

保存页面 diff。

字段：

id
project_id
change_type
field
old_value
new_value
time
project_addresses

保存链上地址。

字段：

id
project_id
address
address_type
chain
first_seen
六、API 设计

新增接口。

获取项目评分

GET /projects/{id}/score

返回：

score
risk_level

获取项目变化

GET /projects/{id}/changes

返回：

变化记录列表。

获取观察列表

GET /watchlist

添加观察项目

POST /watchlist/{project_id}
七、Dashboard

Dashboard 新增三个模块。

项目评分排行

生命周期阶段统计

观察列表

示意：

Top Projects
--------------------------------
AI-Agent-X     score 4
Compute-Net    score 3
DeFi-Compute   score 2

Lifecycle
--------------------------------
detected: 12
launch_announced: 4
launch_open: 1

Watchlist
--------------------------------
AI-Agent-X
Agent-Lab
八、项目目录结构

推荐结构：

virtuals-source
│
├─ app
│  ├─ main.py
│  ├─ config.py
│
│  ├─ sources
│  │   └─ virtuals_source.py
│
│  ├─ lifecycle
│  │   └─ lifecycle_engine.py
│
│  ├─ diff
│  │   └─ diff_engine.py
│
│  ├─ scoring
│  │   └─ score_engine.py
│
│  ├─ address
│  │   └─ address_analyzer.py
│
│  ├─ database
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
九、MVP 范围

v2 第一阶段只需完成：

生命周期追踪

页面 diff

项目评分

观察列表

链上地址解析可以在后续版本增强。