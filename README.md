# SAR Always-In (Binance USD-M Futures Testnet)

生产级架构骨架：WebSocket 只负责接收并入队列，交易执行完全在独立线程中完成；每分钟更新一张 `STOP_MARKET` 反手单，让触发成交发生在交易所侧，降低本地延迟与阻塞风险。

## 1. 依赖安装

```powershell
python -m pip install -r requirements.txt
```

## 2. 配置与密钥

1. 复制 `config.example.json` 为 `config.json` 并按需修改（该文件已在 `.gitignore`）。
2. 设置环境变量（推荐用 PowerShell 临时设置）：

```powershell
$env:BINANCE_API_KEY="你的测试网 key"
$env:BINANCE_API_SECRET="你的测试网 secret"
```

可选安全开关：

- `SAR_TRADING_ENABLED=true|false`：覆盖配置中的 `trading_enabled`（默认不设置时使用配置值）
- `SAR_LOG_LEVEL=INFO|DEBUG`

## 3. 运行

```powershell
python -m sar_bot.main --config config.json
```

## 4. 资金看板 (FastAPI + WebSocket)

后端服务会在同一进程里启动策略线程，并通过 `/ws` 每 0.5s 推送资金/持仓/服务器指标给前端。

启动：

```powershell
$env:BINANCE_API_KEY="你的测试网 key"
$env:BINANCE_API_SECRET="你的测试网 secret"
$env:SAR_TRADING_ENABLED="true"   # 只监控请改为 false
python -m uvicorn sar_web.server:app --host 0.0.0.0 --port 8000
```

访问：

- 浏览器打开 `http://127.0.0.1:8000/`
- WebSocket：`ws://127.0.0.1:8000/ws`
- 启停：`POST /api/toggle`

## 重要说明

- 本项目默认配置为 **Binance USD-M Futures Testnet**：
  - REST：`https://testnet.binancefuture.com`
  - WS：`wss://stream.binancefuture.com`
- `ws_stream_url` 必须填写 **不带** `/ws` 或 `/stream` 的 base（SDK 会自动拼接）。
- 如果你的账户是 Hedge Mode（双向持仓），本机器人默认不交易（`support_hedge_mode=false`），避免出现无法原子反手的复杂状态。
