# 跨主机部署与故障注入

新增 `workflow_engine.deployment`，将中心与业务执行端作为两个独立进程启动。
中心同时提供管理 API、worker RPC 和恢复协调器；中心的 handler 合约不会执行业务。
worker 从受信任的本地 `module:factory` 加载函数，通过 RPC 领取和提交任务，
不接收数据库路径，不共享 SQLite、WAL 或控制文件。

## 两台机器运行 AI 工作流

先在两台机器安装相同版本项目（`python -m pip install .`），或设置 `PYTHONPATH=src`。
证书 SAN 应匹配中心实际域名。以下使用 PowerShell，Linux 可使用对应的环境变量语法。
数据库目录需预先创建并使用中心本地持久磁盘。

中心主机 A：

```powershell
$env:WORKFLOW_RECOVERY_TOKEN = '<部署生成的高熵 worker 密钥>'
$env:WORKFLOW_API_TOKEN = '<不同的高熵管理密钥>'
python -m workflow_engine.deployment central `
  --database D:/workflow-data/workflow.db `
  --handlers llm.chat.v1 llm.validate.v1 `
  --host 0.0.0.0 --api-port 8080 --rpc-port 8090 `
  --cert D:/certs/center.crt --key D:/certs/center.key `
  --max-retries 1
```

业务主机 B：

```powershell
$env:WORKFLOW_RECOVERY_TOKEN = '<与中心一致的 worker 密钥>'
$env:LLM_API_KEY = '<模型供应商密钥>'
$env:LLM_BASE_URL = 'https://api.openai.com/v1'
$env:LLM_MODEL = '<模型名称>'
python -m workflow_engine.deployment worker `
  --server https://center.example.internal:8090 `
  --registry workflow_engine.llm:create_llm_registry `
  --worker-id host-b-1 --host-id host-b `
  --ca D:/certs/ca.crt
```

中心只需 handler 名称，不需要模型供应商凭据。自定义业务可将 worker 的
`--registry` 换成自己的工厂，并让中心 `--handlers` 与其名称集合完全一致。
中心默认不允许无保护的非 loopback HTTP；已经由可信隧道或代理保护时才使用
`--allow-insecure`。安装后也可用 `workflow-deploy` 代替模块入口。

向中心管理 API 提交已有 AI DAG，worker 自动消费：

```powershell
$headers = @{ Authorization = "Bearer $env:WORKFLOW_API_TOKEN" }
Invoke-RestMethod -Uri https://center.example.internal:8080/api/v1/workflows `
  -Method Post -Headers $headers -ContentType 'application/json' `
  -Body (Get-Content examples/llm_validated_workflow.json -Raw)
Invoke-RestMethod -Uri https://center.example.internal:8080/api/v1/workflows/llm-validated-calculation-001 `
  -Headers $headers
```

客户端需信任签发管理 API 证书的 CA。提交、回答、token 用量与事故查询沿用
[管理 API](management_api.md) 和 [LLM 工作流](llm_workflows.md) 的契约。
已有 LLM 节点默认不声明幂等：调用可能产生费用，断电后的重放必须根据业务语义明确决定。

## 独立容器与真实网络故障

`deploy/compose.yaml` 提供中心、victim 和 replacement 三个容器。
三者使用独立进程和网络命名空间；只有中心挂载持久化卷，worker 没有共享卷。
worker 使用只读根文件系统及临时 `/tmp`，无 Docker socket；故障由宿主监督程序注入。
worker 仅连接无外网出口的 `control` 内部网络；中心另外连接 `access` bridge 网络，
用于将管理/RPC 端口发布到宿主 `127.0.0.1` 的随机端口。仅有内部网络时 Docker
可能不分配宿主发布端口，因此访问网络仅交给中心，断开 worker 仍只需切断一条网络。
内部 HTTP 明确限定为本地隔离实验；跨物理主机使用上面的 TLS 入口。

安装并启动 Docker Engine / Docker Desktop（Linux 容器）后，在仓库根目录运行：

```powershell
python tools/validate_cross_host.py --scenario all --output .artifacts/cross-host-report.json
```

也可独立运行 `--scenario network`、`crash`、`stale`、`non-idempotent`。
`--docker` 可指定 Docker CLI 的绝对路径。每次生成唯一 Compose project、随机密钥与
独立数据库卷；结束时清理本次 project 的容器、网络、卷与自动生成的镜像标签。
运行需能拉取 `python:3.12-slim`。

| 场景 | 实际故障与验收 |
| --- | --- |
| network | `docker network disconnect` 切断 victim 唯一网络；中心健康且 replacement 已注册，旧任务仍为 `RUNNING / attempt=1 / retry=0`；连回网络后旧会话不能复活，排空后才恢复 DAG |
| crash | `docker kill --signal KILL` 终止 worker 容器；心跳超时只撤权并打开 `WAIT_STOP`；监督者确认确切容器已退出，replacement 才领取恢复任务 |
| stale | 网络恢复后重发真实旧 owner/session/attempt 的结果和心跳，必须拒绝；同一个 worker ID 用新容器注册新会话后，旧消息仍不能改变新会话或覆盖结果 |
| non-idempotent | 强杀非幂等节点；可信停止后节点失败、工作流暂停，次数保持 `1 / 0`；人工接受失败后工作流结束，下游从未执行 |

每个场景还验证：完成的上游只执行一次，恢复重试只扣一次预算，恢复结果绑定到下游，
中心重启后结果和任务事件不变。重启后重新读取随机发布端口。
验收直接读取 `/proc/self/ns/net`，核验中心和 worker 的内核网络命名空间不同。
故障 worker 在私有 tmpfs 写入业务进入标记，监督者通过 `docker exec` 放行业务；
不依赖固定睡眠推断旧调用仍在运行，中心和 worker 之间也不共享标记文件。
探针使用确定性的 `probe.echo.v1` / `probe.summarize.v1`，
用于隔离部署和恢复语义验收，不进行付费模型调用。LLM HTTP 路径另有下述独立测试。

JSON 报告包含状态、容器 ID、网络、确切停止证据、工作流快照、任务及会话历史，
以及引擎版本、镜像 ID 和内核命名空间证据，不包含密钥。
引擎不可用时返回非零退出码、`status=blocked`；测试失败为 `failed`，
不会降级为本机进程并宣称容器测试通过。首次镜像拉取或构建失败同样不算通过。

## 撤权与停止证明

恢复遵循 `SUSPECT → FENCED → STOPPED → RETRYING`。网络断开只能授权怀疑与撤权。
撤权后旧心跳、领取与结果提交均被拒绝；它不能停止业务在外部系统产生副作用。
worker 在网络故障后停止领取，等待当前调用排空，再尝试提交停止确认。
无法送达确认时，中心继续等待可信监督者。

容器监督程序在注入故障前保存确切 attempt 和会话，确认停止时检查该容器的完整 ID、
`hostname == session.host_id`、`State.Status=exited`、`Pid=0`、未运行/未重启、
`RestartCount=0` 及 `restart=no`，再在中心容器内调用本地确认命令。
不按一个可复用的容器名称或主机名猜测旧执行已经停止。

跨机器监督者完成同等核验后，在中心本地执行：

```powershell
python -m workflow_engine.deployment confirm-stopped `
  --database D:/workflow-data/workflow.db `
  --session-id '<确切旧 session ID>' --host-id host-b `
  --operator deployment-supervisor `
  --evidence '<运行时核验记录：进程/容器 ID、退出码、退出时间、所有旧业务已停止>'
```

该命令只接受已有中心数据库，核验明确的 host/session 归属，并记录恢复审计。
`evidence` 是受信任监督者的声明；通用 CLI 不会自行连接远端运行时验证字符串。
业务启动的后台子进程或异步外部操作也必须纳入监督；停止容器不会撤销已提交给模型
供应商的请求、费用或外部副作用。非幂等任务不自动重放。
中心和本地数据库仍是单一故障域，本次不提供数据库复制或中心自动主备切换。

## 独立测试

各文件每项测试使用新的临时目录和数据库，不依赖其他文件的先后顺序。

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_deployment_network.py -v
python -m unittest discover -s tests -p test_deployment_crash.py -v
python -m unittest discover -s tests -p test_deployment_sessions.py -v
python -m unittest discover -s tests -p test_deployment_llm.py -v
python -m unittest discover -s tests -p test_deployment_supervisor.py -v
```

前三个文件启动独立中心与 worker CLI 进程；网络测试使用可切断的真实 TCP 转发链路，
保持中心服务运行。LLM 测试使用内置 `llm.chat.v1` 从 worker 子进程调用本地 HTTP
供应商桩，验证答案、输出校验、token 去重与预算；中心无供应商密钥。
这些是进程/协议验证，不代替容器网络命名空间隔离验收。
监督器单元测试另外验证仍在运行、主机不匹配、已重启、自动重启策略等证据不会触发停止确认。

容器集成测试显式启用，未启用时会显示 skip：

```powershell
$env:WORKFLOW_CONTAINER_TESTS = '1'
# 可选：$env:WORKFLOW_DOCKER = 'C:/Program Files/Docker/Docker/resources/bin/docker.exe'
python -m unittest discover -s tests -p test_deployment_containers.py -v
```

启用后 Docker 不可用会导致测试失败，不会静默跳过。
Compose 健康依赖、卷与网络采用
[Docker 官方 Compose 文档](https://docs.docker.com/compose/how-tos/startup-order/)、
[网络文档](https://docs.docker.com/compose/how-tos/networking/)中的机制。
