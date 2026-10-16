# 2026-10-16 真实容器部署与故障验收

本轮已完成真实 Docker 容器验收。中心、故障 worker 和替代 worker 使用独立容器与
内核网络命名空间；只有中心挂载 SQLite 持久化卷。验收环境为 Windows 上的
Docker Desktop 4.54.0、WSL 2 Linux 容器、Docker Engine 29.1.2。
本轮验证的是原请求中的隔离环境部署，没有使用两台物理服务器。

## 实际执行结果

`python tools/validate_cross_host.py --scenario all` 返回退出码 0，四项场景均为 `passed`。
原来标为 `blocked` 的报告已由本轮实际结果替换：
[容器验收 JSON](cross-host-container-validation-2026-10-16.json)。

| 场景 | 真实故障及结果 | 恢复节点 attempt / retry |
| --- | --- | --- |
| network | 断开 worker 唯一网络，中心仍健康；旧调用仍存活时不重放；连回后旧会话仍被拒绝，排空后替代 worker 完成 DAG | 2 / 1 |
| crash | SIGKILL 终止容器，退出码 137；停止证明前不重放，监督者确认确切容器退出后恢复 | 2 / 1 |
| stale | 撤权后拒绝旧 owner/session/attempt 的结果、心跳和领取；同一 worker ID 的新容器获得新 session，旧消息不能覆盖新状态 | 2 / 1 |
| non-idempotent | SIGKILL 后保持结果不确定，节点失败且工作流暂停；人工接受失败后结束，下游未执行 | 1 / 0 |

每个场景还实际重启中心，核对原持久化卷中的 DAG 结果和任务事件完全不变。
上游已完成节点均保持一次执行；健康 worker 在没有停止证明时不能接管旧任务。
network/stale 的旧容器因撤权后的请求被拒绝而退出码为 1；强杀场景为 137。

报告保存完整容器 ID、镜像 ID、引擎版本、Docker sandbox ID、`/proc/self/ns/net`
内核命名空间、worker 挂载清单、停止证明、DAG 快照和任务/会话事件。
namespace inode 在不同场景清理后可以复用；每项场景内部都检查中心与 worker 不同。
探针是无外部副作用的确定性业务，不调用付费模型。

## 独立测试与回归

最终全量回归：**456 passed，0 skipped，0 failures，0 errors**，耗时 173.05 秒。
其中新增部署用例为 **15 项通过、0 项跳过**，包含 4 项真实容器测试。
部署用例 JUnit 从同一次完整运行中提取，保留实际 case、时间和状态，未重复计算为另一轮运行。

最终回归显式设置 `WORKFLOW_CONTAINER_TESTS=1`，因此四项容器测试实际执行，
不会因为默认开关而跳过。机器可读证据见
[最终全量 JUnit](cross-host-full-validation-2026-10-16.xml) 与
[部署用例 JUnit](cross-host-independent-validation-2026-10-16.xml)。

部署用例还覆盖真实 TCP 链路分区、独立 CLI worker 进程强杀、错误 host/session
证明拒绝、非幂等恢复、旧会话 RPC、中心重启、错误凭据和停止证明守卫。
内置 `llm.chat.v1` 另在 worker 子进程调用本地 HTTP 供应商桩，验证答案、输出校验、
token 去重及预算；容器故障探针验证隔离和恢复机制，LLM 桩验证内置模型调用路径。

## 实测中修复的问题

- 仅连接 `internal` 网络时，实际 Docker 没有建立发布端口，`compose port` 返回 `:0`。
  中心增加独立 `access` bridge 网络；worker 仍只有内部控制网络，端口仅绑定宿主 loopback。
- 中心重启后随机宿主端口可能重新分配，验收器现在重新读取 API/RPC 地址。
- profile 下的 worker 未被默认 `compose down` 清理，验收器现在显式启用 workers profile，
  完整清理自己创建的容器、网络、卷和自动镜像标签。
- 旧调用存活条件改为 worker 私有 tmpfs 中的进入/释放标记，由宿主 `docker exec`
  监督；不使用固定睡眠作为旧调用仍在运行的证据，也不共享中心数据库或控制卷。
- 本机测试启动探针等待首次协调扫描就绪，允许启动时短暂 503；日志断言等待服务端收尾
  完成后再关闭捕获，仍保留原有响应耗时上限和业务断言。

## 本机运行时恢复记录

Docker 最初分别因 `Docker/run/dockerInference` 与
`docker-secrets-engine/engine.sock` 遗留 socket 无法访问而退出。
确认 Docker 全部退出，且对应目录只含零字节运行时 socket 后，整体改名备份并
创建空目录，随后 Docker 成功启动。没有恢复出厂设置，没有移动 WSL 数据盘、镜像、
容器持久数据或实际密钥数据。

保留的可逆备份路径：

- `C:/Users/Lenovo/AppData/Local/Docker/run.workflow-validation-backup-20261016`
- `C:/Users/Lenovo/AppData/Local/Docker/run.workflow-validation-backup-20261016122154`
- `C:/Users/Lenovo/AppData/Local/docker-secrets-engine.workflow-validation-backup-20261016`

用户原有 Docker Hub 镜像加速器返回 502，因此本轮从
[Docker 官方公共 ECR 镜像库](https://gallery.ecr.aws/docker/library/python)
拉取同版本 Python，再建立本地 `python:3.12-slim` 标签；没有修改全局镜像源。
实际基础镜像摘要为：

```text
sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1
```

部署参数、停止证明边界和复现命令见 [部署文档](cross_host_deployment.md)。
