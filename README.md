# 示波器波形采集服务（FastAPI + SQLite + 双 SCPI 模拟仪器）

单实例实验室采集 API：选择仪器与通道后暂停测量，读取同一次波形的点数、X/Y
定标前导与定长 IEEE 488.2 二进制块（有符号 16 位小端），换算时间与电压点，
同时留存原码与定标；无论成功、超时还是截断，结束时都恢复采集前的运行/暂停状态。

## 目录结构

```
app/                 FastAPI 采集服务
  config.py          环境变量配置、仪器名册
  database.py        SQLite 表结构与状态机持久化（aiosqlite）
  scpi.py            TCP SCPI 客户端 + IEEE 488.2 块解析（含超时/截断处理）
  service.py         每仪器锁、采集编排、恢复与重启恢复
  schemas.py         Pydantic 模型
  main.py            HTTP 路由与生命周期
instrument/          独立 TCP SCPI 模拟示波器（与 API 进程解耦）
  scpi_sim.py        串行命令队列、冻结波形、慢响应/断流故障注入
  main.py            模拟器入口
tests/               pytest 套件（21 项，含协议、服务、HTTP 三层）
Dockerfile           API 与模拟器共用镜像
docker-compose.yml   api + inst1 + inst2 三服务
run_local.sh         无 Docker 时的本地启动脚本
```

## 一键启动（Docker Compose）

```bash
docker compose up --build
# API:        http://localhost:8000
# Swagger:    http://localhost:8000/docs
# OpenAPI:    http://localhost:8000/openapi.json
# inst1 SCPI: localhost:5025   inst2 SCPI: localhost:5026
```

无 Docker 环境（本机依赖装在 `./.pydeps`）：

```bash
./run_local.sh
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| GET  | `/instruments` | 仪器名册、忙/排队、门控、实时 RUN/STOP、上次恢复 |
| POST | `/instruments/{name}/acquisitions` | 发起采集，body `{"channel": 1}`，返回 202 与 id |
| GET  | `/acquisitions/{id}` | 采集状态（PENDING/IN_PROGRESS/RESTORING/SUCCESS/FAILED） |
| GET  | `/acquisitions/{id}/result` | 成功后的波形；失败返回 409，不提供部分波形 |
| GET  | `/acquisitions?instrument=&limit=` | 历史列表 |

采集示例：

```bash
curl -X POST http://localhost:8000/instruments/inst1/acquisitions \
  -H 'Content-Type: application/json' -d '{"channel": 1}'
curl http://localhost:8000/acquisitions/1
curl http://localhost:8000/acquisitions/1/result
```

## 采集与恢复语义

1. 先查询并记录仪器当前 `:RUN?` 状态（RUNNING/STOPPED），再 `:STOP` 暂停。
2. 暂停后依次读取点数、X 原点/增量、Y 原点/增量前导，再读 IEEE 488.2 定长块；
   块按 int16 小端解包，点数与前导不一致即判失败。
3. 换算 `t[i] = XOR + i * XINC`、`V[i] = YOR + code[i] * YINC`，原码与定标一并入库。
4. 恢复在终态写库**之前**完成：原 RUNNING 则 `:RUN` 并回读确认；原 STOPPED
   只发幂等 `:STOP`，**绝不擅自启动**。确认通过才置 SUCCESS，否则 FAILED。
5. 超时、块截断、连接重置等任何失败都走同一条恢复路径（有界重试 + 新建连接），
   失败记录不写任何部分波形，`/result` 返回 409。
6. 每仪器一把异步锁串行化所有操作，同仪器并发不会串通道/定标；两台仪器锁独立，
   慢响应或等待恢复不阻塞另一台。
7. API 重启时，所有非终态行在启动阶段标记为 `FAILED/API_RESTARTED`，并同步恢复
   对应仪器后才开放流量；恢复确认前置 `restored=0`，确认后回写 `restored=1`。

## 模拟器特性与故障注入

模拟器与 API 是不同容器/进程，**API 重启不会重置仪器**。命令按仪器串行执行
（全仪器单队列），已接收命令在断线后仍会执行完。支持：

| SCPI 命令 | 作用 |
| --- | --- |
| `*IDN?` `*OPC?` `:BUSY?` | 标识、操作完成、排队忙状态 |
| `:RUN` `:STOP` `:RUN?` | 运行/暂停控制与查询 |
| `:CHAN<n>:POS?` | 通道存在性 |
| `:WAV:POIN?` `:WAV:XOR?` `:WAV:XINC?` `:WAV:YOR?` `:WAV:YINC?` | 前导 |
| `:WAV:DATA?` 或 `:WAV:DATA? (@2)` | IEEE 488.2 块（int16 LE），暂停时冻结 |
| `:SYST:DELAY <秒>` / `?` | 每条命令注入慢响应 |
| `:SYST:CUT 1` / `?` | 下一次 `:WAV:DATA?` 发到一半断流（一次性） |
| `:SYST:DROP <n>` / `?` | 后续 n 条命令直接断连（跨连接生效） |
| `*RST` | 清除故障注入（保留波形状态） |

故障注入示例：

```bash
# 慢响应
printf ':SYST:DELAY 0.5\n' | nc localhost 5025
# 断流
printf ':SYST:CUT 1\n'   | nc localhost 5025
```

## 测试

```bash
PYTHONPATH=.pydeps:. python3 -m pytest tests -q
```

覆盖：协议解析与块截断、慢响应超时、断线后命令续执行、串行不交错、完整成功
采集、原暂停仪器不被启动、截断/超时失败与恢复、同仪器并发串行化、跨仪器并行
不阻塞、重启中断标记与恢复、HTTP 全流程与 404/409/422。
