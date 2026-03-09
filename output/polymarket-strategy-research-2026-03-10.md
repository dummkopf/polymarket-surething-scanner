# Polymarket 自动化策略深度探索（仅策略研究，不实现）

> 时间：2026-03-10 07:54+08:00  
> 目标：探索“可自动化、可执行、竞争不太高、具备结构性优势”的 Polymarket bot 策略，并评估是否真的可行/有利可图。  
> 结论先行：**最靠谱的方向不是“预测谁会赢”，而是“吃微结构与机制红利”**，尤其是：
> 1) 奖励函数驱动的双边做市  
> 2) 负风险（Neg Risk）事件的跨结果一致性交易  
> 3) 临近结算期的机制性收敛（有明确风控前提）

---

## 一、信息源分级与可信度

## A级（高可信，官方/学术）
1. Polymarket Docs - CLOB Overview  
   https://docs.polymarket.com/developers/CLOB/introduction
2. Polymarket Docs - Trading Fees  
   https://docs.polymarket.com/trading/fees
3. Polymarket Docs - Maker Rebates Program  
   https://docs.polymarket.com/market-makers/maker-rebates
4. Polymarket Docs - Liquidity Rewards  
   https://docs.polymarket.com/market-makers/liquidity-rewards
5. Polymarket Docs - Negative Risk Markets  
   https://docs.polymarket.com/advanced/neg-risk
6. arXiv 1009.1446 - Comparing Prediction Market Structures (LMSR vs BMM)  
   https://arxiv.org/abs/1009.1446
7. arXiv 2405.11444 - Adaptive Optimal Market Making Strategies  
   https://arxiv.org/abs/2405.11444

## B级（中可信，社区技术文）
1. DEV 文章（midpoint fee-aware 做市）  
   https://dev.to/benjamin_martin_749c1d57f/building-a-midpoint-trading-bot-strategy-for-polymarket-fee-considered-market-making-in-2026-4lbc
2. GitHub: advaricorp/Polymarketbot（需代码级复核）  
   https://github.com/advaricorp/Polymarketbot

## C级（低可信，营销/泛教程）
- DuckDuckGo 检索到若干“赚钱指南/教程站”，大多偏营销，不能直接当 alpha。

> 处理原则：**A级用于建模框架，B级用于可执行细节启发，C级只做线索，不做结论依据。**

---

## 二、先校准现实：Polymarket 上“结构性优势”到底来自哪里？

基于官方文档，结构优势主要来自四类“机制差”：

1. **费率曲线非线性**（taker fee 在 50% 附近最高，向两端递减）  
   - 见 Fees + Maker Rebates 文档。  
   - 含义：报价位置、成交方向和库存结构会显著影响净收益。

2. **做市激励不是只看成交**，而是看**挂单质量 + 双边深度 + 离中间价距离**  
   - Liquidity Rewards 采用评分函数，且在 [0.10, 0.90] 区间允许单边但折扣；极端区间需要双边才有分。  
   - 含义：会形成“奖励驱动型微结构 alpha”，不是传统单纯吃 spread。

3. **负风险（Neg Risk）事件可转换**  
   - No(某结果) 可转成其他结果的 Yes 组合。  
   - 含义：多结果事件存在可编程的一致性约束，给了“关系型套利”空间。

4. **CLOB 是混合架构（offchain match + onchain settle）**  
   - 低延迟撮合 + 链上最终性。  
   - 含义：自动化执行可行，但也意味着你会和真正的程序化对手竞争。

---

## 三、候选策略池（按“靠谱程度”排序）

评分维度（5分制）：
- 可自动化（A）
- 可执行性（E）
- 利润潜力（P）
- 竞争强度（C，分越高竞争越低）
- 结构优势（S）

### 策略1：奖励函数感知的双边做市（优先）
**定义**：围绕 size-adjusted midpoint，动态双边挂单，目标是“spread + rebate + liquidity rewards”三层收益。  
**机制依据**：官方 Fees / Maker Rebates / Liquidity Rewards 文档。

**为什么靠谱**
- 不是拍脑袋方向判断，而是吃平台公开激励函数。  
- 能通过参数化改进：quote 距离、双边对称性、库存上限、re-quote 频率。

**核心可行性条件**
- 严格 post-only/maker-only（避免 taker 费侵蚀）。  
- 奖励分公式实盘回放（按 sample 近似重建 Q 分）。  
- 库存偏斜时自动 widening + 节制对冲。

**主要风险**
- 同质化竞争（尤其热门市场）。  
- 奖励规则或市场结构变化。  
- 极端行情下库存挤压。

**评分**：A=5 E=5 P=3.5 C=2.5 S=4.5

---

### 策略2：Neg Risk 一致性套利（高潜力、实现复杂）
**定义**：在 `negRisk=true` 的多结果事件中，监控“可转换组合”的定价偏离，执行低风险组合单。  
**机制依据**：官方 Negative Risk 文档。

**为什么靠谱**
- 这是平台机制层面的“约束套利”，比主观预测更稳。  
- 很多参与者不做跨市场联动，错价修复速度可能慢于主流盘口。

**核心可行性条件**
- 必须正确建模转换关系与交易成本（含 fee/slippage）。  
- 必须支持多腿联动/失败回滚逻辑。  
- 事件级风险：占位 outcome、规则变更、结算异常要有 kill switch。

**主要风险**
- 多腿执行失败导致裸露敞口。  
- 深度不足导致理论 edge 无法兑现。  
- 规则细节理解错误会把“套利”做成“方向赌”。

**评分**：A=4 E=3.5 P=4.5 C=4 S=5

---

### 策略3：临近到期收敛 + 流动性不对称（你已验证方向）
**定义**：在临近结算窗口，筛选“价格-外部概率偏离 + 流动性可成交”的合约，做小尺寸收敛单。  
**机制依据**：你现有 weather 体系 + 订单簿执行经验。

**为什么靠谱**
- 到期驱动是硬约束，减少长期叙事噪音。  
- 可与风险闸门（max exposure/day stop loss）天然结合。

**核心可行性条件**
- 不能重复同市场同方向无限加仓。  
- 对“边际优势衰减”有明确退出规则（edge floor + holding limit）。  
- 必须有盘口快照序列，否则回测失真。

**主要风险**
- 盘口稀疏，进得去出不来。  
- 标的信息冲击临近结算更剧烈。  
- 看似收敛，实则被新信息重定价。

**评分**：A=4.5 E=4.5 P=3.5 C=3.5 S=4

---

### 策略4：事件簇相关性对冲（中等靠谱）
**定义**：同主题相关市场（如同赛事/同宏观事件）之间做相对价值而非绝对方向。  
**为什么可考虑**
- 竞争者普遍做单市场信号，跨市场协方差建模门槛更高。  

**风险**
- 相关性在关键时刻会断裂。  
- 需要更高的数据质量和风控框架。

**评分**：A=3.5 E=3 P=3.5 C=4 S=3.5

---

### 策略5：纯“社媒情绪抢跑”（不建议作为主策略）
**定义**：抓 X/新闻情绪信号抢先下单。  
**问题**
- alpha 半衰期极短，基础设施门槛高。  
- 容易退化为噪声交易。  

**评分**：A=4 E=3 P=2.5 C=1.5 S=2

---

## 四、对“网上文章”的反向审查（不 take as granted）

以 DEV 的 midpoint 文为例（B级源）：
- 可取：maker-only、fee-aware、库存控制、双边报价的实践框架。  
- 不可直接信：收益率叙述（1-8%月化/10-25%年化等）没有统一可复现实验设计。  
- 结论：**可当“执行模板”，不能当“收益承诺”。**

以 GitHub Polymarketbot 为例：
- 可取：模块化工程结构示意。  
- 不可直接信：仓库叙述偏“企业级宣传”，需代码与历史提交验证真实度。  
- 结论：**可当脚手架参考，不可当 alpha 来源。**

---

## 五、学术洞察如何落地到 Polymarket

1. **BMM vs LMSR（arXiv 1009.1446）**  
   关键启发：流动性、稳定性、适应性有 trade-off。  
   落地：做市参数不应固定（固定偏移会被市场状态切换打爆），应“状态感知”。

2. **Adaptive MM（arXiv 2405.11444）**  
   关键启发：需求随机性 + 自适应挂单优于固定价差。  
   落地：把“book 失衡、成交到达率、短期波动”做成实时调参输入。

> 重点：学术不是让你照搬公式，而是告诉你 **静态规则会死，状态自适应才有生命力**。

---

## 六、我给你的推荐路线（仅 research 版结论）

### 最优组合（按优先级）
1. **主线A：奖励函数感知做市**（最稳的机制红利）  
2. **主线B：Neg Risk 关系套利**（最像结构性优势）  
3. **辅线C：到期收敛策略**（你已有基础，迭代成本低）

### 不建议作为主线
- 纯新闻/纯情绪预测  
- 泛“AI预测胜率”而无微结构执行优势

---

## 七、是否“有利可图”的判定标准（避免自嗨）

后续你 review 时，建议用这组门槛（研究阶段）：

1. 单策略净收益拆解必须可解释：  
   `Net = Spread + Rebate + Reward - Fees - Slippage - Inventory Drag`
2. 参数扰动鲁棒：主要参数 ±20% 仍不崩。
3. 竞争敏感性测试：假设成交率下降30%，是否仍正期望。
4. 极端场景回放：盘口骤薄/跳价时最大回撤是否可接受。
5. 资本效率：单位风险资本的收益是否显著高于被动持仓。

---

## 八、本轮研究缺口（下一轮应补）

1. **X 帖子深挖缺口**：当前环境未完成 X API 认证，无法直接抓 thread 内容做作者级策略拆解。  
2. **官方奖励字段实时数据样本**：需要持续抓取 market-level 参数，做横截面对比。  
3. **Neg Risk 实盘可成交性画像**：需要事件级深度/滑点样本，确认理论 edge 可兑现。

---

## 九、给 Kai 的一句话结论

如果你要“竞争不太高 + 可自动化 + 真正 structural edge”，**优先做平台机制型策略（奖励函数做市 + neg risk 关系套利）**，把“预测谁会赢”降级为辅助信号，而不是主收益引擎。
