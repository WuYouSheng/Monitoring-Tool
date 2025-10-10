# OpenTelemetry Remote Metrics Export 設定指南

## 功能說明
新增的OTel導出功能會將本地Grafana中的所有QoS指標同步推送到遠端Grafana，確保本地和遠端顯示完全一致。

## 推送的指標
- **FPS**: 總FPS、2D FPS、3D FPS
- **Throughput**: 總吞吐量
- **Network Latency**: 網路延遲直方圖
- **Center Processing Latency**: Center處理延遲直方圖
- **Availability**: 系統可用性比率
- **Status Flags**: FPS狀態、網路狀態等二進制指標

## 安裝依賴
```bash
cd Center
pip install -r requirements.txt
```

## 配置設定

### 1. 啟用OTel導出
在 `center_config.json` 中已配置：
```json
{
  "otel": {
    "enabled": true,
    "endpoint": "125.227.156.108:26001",
    "service_name": "agent-tool-center",
    "export_interval_ms": 1000,
    "export_timeout_ms": 1000
  }
}
```

### 2. 配置參數說明
- `enabled`: 啟用/停用OTel導出
- `endpoint`: 遠端OTel接收端點
- `service_name`: 服務識別名稱
- `export_interval_ms`: 推送間隔（毫秒）
- `export_timeout_ms`: 推送超時（毫秒）

## 使用方式

### 1. 啟動Center服務
```bash
cd Center
python server.py
```

### 2. 檢查日誌
正常啟動會看到：
```
[OTel] Initialized exporter to 125.227.156.108:26001
HTTP server started on http://0.0.0.0:444
```

### 3. 停用OTel（如需要）
修改 `center_config.json`：
```json
{
  "otel": {
    "enabled": false
  }
}
```

## 技術特點

### 1. 非侵入式設計
- **原功能完全不受影響**: 所有InfluxDB寫入和本地Grafana功能保持不變
- **錯誤隔離**: OTel推送失敗不會影響主要功能
- **可選功能**: 可隨時啟用/停用

### 2. 數據一致性
- **同源數據**: 使用相同的metrics計算結果
- **同步推送**: 與本地寫入同時進行
- **完整覆蓋**: 所有Grafana顯示的指標都會推送

### 3. 容錯設計
- **自動降級**: OpenTelemetry庫不可用時自動停用
- **異常處理**: 網路問題不會導致服務崩潰
- **優雅關閉**: 程序結束時確保數據推送完成

## 遠端Grafana設定
遠端Grafana需要配置OTel數據源以接收推送的metrics，具體配置需要根據遠端環境調整。

## 故障排除

### 1. OTel庫未安裝
```
[WARNING] OpenTelemetry not available. Remote metrics export disabled.
```
解決：執行 `pip install -r requirements.txt`

### 2. 網路連接問題
```
[OTel] Host export failed for 192.168.20.5: [connection error]
```
解決：檢查網路連接和防火牆設定

### 3. 關閉OTel功能
設定 `"enabled": false` 即可完全停用推送功能。