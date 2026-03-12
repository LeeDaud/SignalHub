# SignalHub - Virtuals Monitor

基于 FastAPI + SQLite + APScheduler 的 Virtuals 新项目监听 MVP。

## 能力

- 轮询 Virtuals 项目列表
- 识别新项目
- 检测项目字段与状态变化
- 生成标准事件并写入 SQLite
- 提供 FastAPI 查询接口
- 提供极简 Dashboard

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

默认使用 `sample_data/virtuals_projects.json` 作为模拟数据源。

切换到真实接口时，设置：

```bash
VIRTUALS_ENDPOINT=https://your-endpoint.example/api/projects
VIRTUALS_SAMPLE_MODE=false
POLL_INTERVAL_SECONDS=30
```
