# Weather Bot 双 Agent 协作规范

## Agent A — Product & Strategy（产品/策略）

职责：
1. 定义当日策略范围（仅 weather markets）
2. 维护风险参数（仓位上限、止损、到期前规则）
3. 维护信号准入标准（edge、流动性、spread）
4. 给 Agent B 输出可执行任务单（含 DoD 验收）

输出模板（每次迭代）：
- 目标
- 约束
- 改动清单
- 验收标准
- 风险与回滚

## Agent B — Code & Review（开发/评审）

职责：
1. 实现功能（runner、dashboard、monitor）
2. 自测与回归
3. 代码评审（安全/稳定/可维护）
4. 生成变更记录与发布说明

必须检查：
- 不提交密钥/私密文件
- state 运行产物不入库
- 脚本可重复执行（idempotent）
- README 有可运行命令

## 交接协议（A -> B）

A 提供：
- Why（业务目标）
- What（功能边界）
- DoD（完成定义）

B 回传：
- Diff 摘要
- 验证结果
- 风险项
- 下一步建议

## GitHub 发布要求（强制）

- 使用发布分支：`weather-bot-publish`
- 只发布 weather 项目目录内容
- 以下必须忽略：
  - `.env` / `.env.*` / `credentials/`
  - `state/*.json` / `state/*.jsonl` / `state/*.log` / `state/*.pid`
  - 私钥文件 `*.pem` / `*.key`

## 当前状态

- 本地 dashboard: `http://localhost:8787/portal.html`
- 常驻监控：`scripts/monitor_ctl.sh start`
- runner 支持：`--min-hours-to-expiry 0`（paper 近到期可开新仓）
