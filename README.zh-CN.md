# openSensus

[English](README.md) | [简体中文](README.zh-CN.md)

openSensus 是面向 AI Agent 的感知与注意力层，也是
[`sensus/0.1`](docs/sensus-protocol-v0.1.md) 协议的参考实现。

企业系统提交只追加（append-only）的 Observation，openSensus 将其投影为当前状态、检测有证据
支撑的 Signal，并通过 MCP 暴露一个带上限、按权限过滤的世界视图——这样定时唤醒的 Agent
只问一次，就能知道发生了什么变化，而不必自己去轮询每一个数据源。

```text
GitLab / Jira / CRM
        |
        | Observation API
        v
   openSensus Runtime ---------> 世界投影
        ^                           |
        | openSensus MCP            | Signal + 证据
        |                           |
   定时 Agent <---------------------+
```

openSensus 不替代特定数据源的 MCP Server。它告诉 Agent 去哪里看、为什么值得看，然后把深度
排查移交出去：`get_evidence` 返回一个已验证的能力名，例如
`gitlab.merge_request.read`，由 Harness 把它映射到已安装的垂直 MCP。openSensus 不代理源系统
凭据，也不执行源系统动作。

## 当前 MVP 已实现的能力

- `POST /v1/observations` 与 `/v1/observations/batch`
- 严格的 `sensus/0.1` 校验，含幂等与冲突检测
- 实体、关系、状态、事件与指标的投影，可跑在 SQLite 或 PostgreSQL 上
- 针对当前状态的迟到数据保护
- 基于 principal 与密级的 ACL 过滤，包括实体字段级 ACL
- 修正 Observation 与确定性的 subject 投影重放
- 显式的快照 / 对账会话，以及安全的权威删除
- 可配置的相对变化与阈值 Signal 规则
- 有界、防环的关系图展开，用于组织层级汇总
- 六个只读 MCP 工具：`observe`、`inspect`、`timeline`、`query`、`compare`、
  `get_evidence`
- 证据解析器提示，用于把深度排查移交给垂直 MCP Server
- 覆盖 HTTP、存储、投影、Signal、证据与 MCP 的自动化集成测试，其中并发套件已**验证过去掉
  修复即会失败**

它闭合的闭环：

```text
Observation
  -> 带权限语义的投影
  -> 修正 / 对账
  -> 可配置的检测
  -> 有界的图观察
  -> MCP 排查
  -> 已验证的证据 / 移交垂直 MCP
```

## 文档

| 文档 | 内容 | 语言 |
| --- | --- | --- |
| [接入文档](docs/integration-guide.zh-CN.md) | 动手向指南：运行 Runtime、编写 Producer、接入 Agent、配置规则、运维与故障排查 | [EN](docs/integration-guide.md) · [中文](docs/integration-guide.zh-CN.md) |
| [架构说明](docs/architecture.zh-CN.md) | Runtime 如何工作以及为何这样设计：日志/投影分离、按类型的合并语义、修正、对账、Signal 检测、访问控制。含架构图与时序图 | [EN](docs/architecture.md) · [中文](docs/architecture.zh-CN.md) |
| [协议 v0.1](docs/sensus-protocol-v0.1.md) | 规范性接口契约：Observation 信封、HTTP 摄入、MCP 工具、合规要求 | [EN](docs/sensus-protocol-v0.1.md) |

协议规范是规范性文件，仅维护英文版本，以保证契约表述的唯一权威性。

## 环境要求

- Node.js 22 或更高
- npm

## 快速开始

安装并构建：

```bash
npm install
npm run build
```

导入一个 GitLab review 延迟场景的演示数据：

```bash
SENSUS_DB_PATH=./data/demo.db npm run seed
```

启动摄入 API：

```bash
SENSUS_DB_PATH=./data/demo.db \
SENSUS_TENANT_ID=acme \
SENSUS_API_KEY=local-secret \
SENSUS_PRINCIPALS=role:agent,team:payments \
SENSUS_CLEARANCE=internal \
npm start
```

API 默认监听 `http://127.0.0.1:8787`。可以用下面的命令检查：

```bash
curl -H 'Authorization: Bearer local-secret' \
  http://127.0.0.1:8787/health
```

> 注意：`/health` 也走同一套 Bearer 校验。设置了 `SENSUS_API_KEY` 后，不带鉴权的健康
> 检查会返回 `401`。

本地开发时，省略 `SENSUS_API_KEY` 即可关闭 Bearer 鉴权。默认绑定地址仍为 localhost。

## 接入 MCP Server

先构建，然后配置 MCP 宿主来启动它：

```bash
SENSUS_DB_PATH=/absolute/path/to/data/demo.db \
SENSUS_TENANT_ID=acme \
node /absolute/path/to/openSensus/dist/src/mcp.js
```

通用 MCP 宿主配置：

```json
{
  "mcpServers": {
    "openSensus": {
      "command": "node",
      "args": ["/absolute/path/to/openSensus/dist/src/mcp.js"],
      "env": {
        "SENSUS_DB_PATH": "/absolute/path/to/data/demo.db",
        "SENSUS_TENANT_ID": "acme"
      }
    }
  }
}
```

租户选择是进程配置，而不是模型生成的 MCP 参数。这样可以防止 Agent 通过工具入参选择其他
租户。MCP 进程还接收可信的 Consumer principals 与最大密级：

```text
SENSUS_PRINCIPALS=role:agent,team:payments
SENSUS_CLEARANCE=internal|confidential|restricted
```

显式 `deny` 优先于 `allow`；存在显式 `allow` 时，至少需要有一个 principal 匹配。
`inherit_from_source: true` 而 `allow`/`deny` 元数据未解析时，为 fail-closed。

## 提交一条 Observation

```bash
curl -X POST http://127.0.0.1:8787/v1/observations \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "spec_version": "sensus/0.1",
    "observation_id": "obs_example_1",
    "tenant_id": "acme",
    "kind": "state.observed",
    "subject": {
      "type": "software.change",
      "id": "gitlab:acme/payments-api!3812"
    },
    "occurred_at": "2026-09-16T09:10:00Z",
    "observed_at": "2026-09-16T09:10:04Z",
    "source": {
      "system": "gitlab",
      "instance": "acme-gitlab"
    },
    "data": {
      "field": "software.review_status",
      "operation": "set",
      "value": "waiting"
    }
  }'
```

## 快照对账

开启一次源快照：

```bash
curl -X POST http://127.0.0.1:8787/v1/syncs \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "sync_id": "sync_gitlab_20260917",
    "mode": "reconciliation",
    "source": { "system": "gitlab", "instance": "acme-gitlab" },
    "authoritative_deletion": true
  }'
```

带上如下请求头提交快照内的 Observation：

```text
Sensus-Sync-Id: sync_gitlab_20260917
```

然后完成快照：

```bash
curl -X POST \
  http://127.0.0.1:8787/v1/syncs/sync_gitlab_20260917/complete \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{ "record_count": 1842, "cursor": "gitlab:2026-09-17T10:00:00Z" }'
```

删除是保守的。只有当已完成的快照是权威的、且没有其他来源仍报告该实体存在时，才会把实体
标记为已删除。如果先前被删除的源记录重新出现，对账会发出一条可审计的存在性 Observation
并恢复它。

`record_count` 必须等于实际挂载的成员数，否则完成请求会被拒绝——这道校验可以防止一份被
截断的快照误删大量存活实体。

## 配置 Signal 规则

规则按租户持久化，并立即重新评估已有的指标序列：

```bash
curl -X POST http://127.0.0.1:8787/v1/signal-rules \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "rule": {
      "rule_id": "review_wait_slo",
      "name": "Review wait exceeds seven hours",
      "enabled": true,
      "applies_to": {
        "metric": "software.review_wait_time",
        "subject_types": ["organization.team"],
        "dimensions": {}
      },
      "condition": {
        "kind": "threshold",
        "operator": "gt",
        "value": 7,
        "for_samples": 2
      },
      "signal_type": "software.review_wait_slo_breach",
      "severity": "critical",
      "confidence": 1
    }
  }'
```

支持的条件为绝对 `threshold` 与基于百分比的 `relative_change`，两者都可要求连续命中多个
采样。

## 关系图观察

`observe` 可以汇总相关实体，同时保持有界：

```json
{
  "scope": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "include": ["state", "changes", "signals"],
  "expand": {
    "direction": "incoming",
    "relations": ["organization.owned_by", "software.belongs_to"],
    "max_depth": 2,
    "max_nodes": 100
  }
}
```

遍历防环、按 ACL 过滤，并封顶在五层深度与 500 个节点。

## 开发

```bash
npm run check
npm test
npm run dev            # 摄入 API，热重载
npm run mcp:dev        # stdio MCP
npm run mcp:http:dev   # Streamable HTTP MCP
```

`npm test` 会先构建项目，然后用 Node 内置测试运行器跑集成测试。存储一致性测试与
PostgreSQL 端到端测试在没有可达数据库时会自行跳过，因此在没有数据库的机器上该命令仍然
全绿。

设置 `SENSUS_TEST_DATABASE_URL` 与 `SENSUS_TEST_DATABASE_URL_E2E` 可让它们针对真实的
PostgreSQL 实例运行。具体做法以及 CI 强制的 lockfile 规则见
[CONTRIBUTING.md](CONTRIBUTING.md)。

## 安全

部署到笔记本之外的任何环境之前，请先读 [SECURITY.md](SECURITY.md)。其中列出了安全模型
所依赖的前提——生产者被信任可以描述自己的数据、摄入 API 不区分角色、租户隔离属于配置
而非默认行为——你可以据此判断这些前提在你的部署中是否成立。

漏洞请私下报告，不要开公开 issue。

## 许可证

MIT——见 [LICENSE](LICENSE)。
