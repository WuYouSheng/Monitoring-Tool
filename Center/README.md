# Agent-Tool Center Stack

## 1. 以 Docker 啟動 InfluxDB 與 Grafana

前置需求：已安裝 Docker 與 Docker Compose。

```bash
cd /Users/wuyousheng/Desktop/PythonProject/Agent-Tool/Center
docker compose up -d
```

啟動後：
- InfluxDB 2.x：`http://localhost:8086`（帳密：agent / agentpass，Token 已設為 `agent-tool-token`）
- Grafana：`http://localhost:3000`（預設帳密：`admin` / `admin`）

Grafana 已自動匯入 InfluxDB Data Source 與 QoS Dashboard（Agent Tool QoS）。

## 2. 調整 Center 端設定

編輯 `center_config.json`：
- `influxdb.host` 建議保持 `localhost`（容器內部已透過 datasource 指向 `influxdb` 服務名）
- `token` 須與 docker-compose 初始化一致（預設 `agent-tool-token`）
- `websocket_server_port`（預設 444）

## 3. 啟動 Center 服務

```bash
pip install -r requirements.txt
python server.py
```

## 4. 啟動 Sender 端

```bash
cd /Users/wuyousheng/Desktop/PythonProject/Agent-Tool/Sender
pip install -r requirements.txt
python sender.py
```

確保要傳送的 JSON 置於 `wb_path` 與 `grpc_path` 目錄，未附帶 `SENT` 的會被送出並在成功後重新命名為 `*SENT.json`。

## 5. Grafana 觀察

在瀏覽器開啟 `http://localhost:3000`，登入後找到 `Agent Tool QoS` 儀表板，觀察 FPS、Throughput、延遲與可用性。


