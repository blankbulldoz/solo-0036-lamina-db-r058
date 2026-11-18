# LaminaDB 事务使用指南：交互式编辑与后台历史清理共存

本指南面向把 LaminaDB 嵌入工作流的调用方，回答一个具体问题：**当交互式编辑会话与后台 `vacuum` 历史清理同时运行时，应当怎样持有事务，才能既有稳定的读取依据，又不被保留成本拖垮。**

文中每一条结论都对应三类材料之一：

- **源码保证**：指出实现该语义的具体函数与文件；
- **实际观测**：来自 `examples/txn_strategy_lab.py` 的真实 HTTP 返回（一次性临时实例，可整段复现）；
- **调用方约束**：数据库不承担、必须由应用自己保证的部分。

本文不使用“全部串行执行”这种笼统说法。LaminaDB 确实有一把全局锁，但它只串行化**单个请求处理段**，不串行化一个事务从 begin 到 commit 的完整生命周期；隔离级别是快照隔离（SI）加同键 first-committer-wins，不是串行化（见 §7）。

---

## 1. 先建立心智模型：序号、版本链、活动事务与回收水位

四个引擎状态共同决定一次读能看到什么（`internal/engine/database.go:15`）：

| 状态 | 含义 | 源码 |
| --- | --- | --- |
| `sequence` | 已发布的最高提交序号，单调递增 | `DB.publish` 先赋 `sequence+1` 再落盘应用，`database.go:99` |
| `store` | 每个关系、每个主键一条按序号排列的版本链，版本含完整行或墓碑 | `versions/store.go:26` |
| `transactions` | 活动事务表；每个事务在 begin 时固定 `Snapshot = d.sequence` | `engine/transaction.go:42` |
| `horizon` | 持久回收水位，只能前进不能回退 | `engine/maintenance.go:79`、`engine/replay.go:58` |

读取可见性由 `versions.Visible` 用二分查找选出链上 `sequence <= at` 的最后一个版本，墓碑视为不可见（`versions/store.go:32`）。因此：

- 事务内查询的 `at` 就是 begin 时固定的快照序号，重复读稳定，并且能读到自己暂存的写入（`engine/read.go:28`、`overlay` 于 `engine/replay.go:69`）；
- 无事务普通读使用**请求时刻**的当前序号，两次普通读之间若有提交，依据就会变；
- `at` 一次性查询不创建事务、不固定任何东西，只接受当时保留区间 `[horizon, sequence]` 内的序号（`engine/read.go:32`），且要求关系在该序号已存在（`engine/read.go:35`）。

**关键推论（本指南的核心）：一个旧版本是否能删，不取决于“当前值是否已经更新”，而取决于“是否还有活动事务的快照需要它，以及它是否是水位时刻必须保留的最后版本”。** 版本链的保留规则见 §4。

---

## 2. 两种工作流如何共同维护活动读取视图

### 2.1 交互式编辑工作流

`POST /transactions` 返回的事务对象包含 `id`、`snapshot`、`read_only`、`started`（`engine/transaction.go:17`）。在 commit/rollback 之前：

- 所有 `put`/`delete` 只进入私有写集 `tx.writes`，其他读者看不到（walkthrough `txn-write` 断言提交前 `matched` 不变）；
- 所有带 `transaction` 的查询都在同一个 `snapshot` 上执行，并叠加私有写集（read-your-writes，`engine/read.go:55`）；
- 事务没有自动过期（README「资源与错误」），只要不显式关闭，它就一直占据活动事务表并持续固定水位。

### 2.2 后台历史清理工作流

`POST /vacuum {"horizon": N}` 的实际水位不是调用方说了算。引擎取“请求值与所有活动事务快照的最小值”（`engine/maintenance.go:82-87`）：

```go
horizon := requested
for _, tx := range d.transactions {
    if tx.Snapshot < horizon {
        horizon = tx.Snapshot
    }
}
```

当存在快照更早的活动事务时，返回体 `pinned=true`，且 `horizon` 被压低。**只读事务一样会固定水位**——pin 只看 `Snapshot`，不看 `ReadOnly`。这就是两条工作流的耦合点：编辑会话的寿命直接决定清理能推进到哪里。

### 2.3 一次真实的 pin 观测（Lab 1）

种子两行（序号 2），编辑事务在序号 2 开启，另一个写者把 id=1 改成 150（序号 3），随后请求把水位推到序号 4：

```text
vacuum-while-editor-open → 200 {"horizon": 2, "removed": 0, "retained": 3, "pinned": true}
editor-repeatable-read   → id=1 balance=100   （固定在快照 2）
current-read             → id=1 balance=150   （plan.snapshot=4，当前视图）
```

编辑事务 rollback 后再清理，同一批旧版本才真正被回收，且编辑器原来的快照序号立刻变得不可读：

```text
vacuum-after-editor-closed       → {"horizon": 4, "removed": 1, "retained": 2, "pinned": false}
time-travel-to-editor-snapshot   → 410 snapshot unavailable; retained interval 4..5
```

注意第一次 vacuum 虽然什么都没删，仍然发布了一条 `vacuum` 记录并消耗了一个序号（序号 4）；`/stats` 里 `journal_records` 同步增长。**vacuum 压缩的是内存版本链，不清日志磁盘占用**（README「变更与回收」；本指南 §8 详述）。

---

## 3. 两种事务持有策略（可复现交错对比）

下面两个策略在同一 schema、同一种子（`accounts(id int 主键, balance int, note text 可空)`，两行各 100，种子提交在序号 2）上比较。完整脚本：`examples/txn_strategy_lab.py`，运行方式见 §9。

### 策略 A：长事务——一个会话一个事务，编辑期间持续持有

**交错（Lab 1 的确定性时序）**

| 步骤 | 会话/操作 | 事实 |
| --- | --- | --- |
| 1 | 编辑事务 begin，`snapshot=2` | 活动读取视图固定 |
| 2 | 后台写者提交 id=1→150，序号 3 | 编辑器视图不变 |
| 3 | vacuum 请求 horizon=4 | 被 pin 到 2，`removed=0` |
| 4 | 编辑器多次查询 | 始终读到 balance=100，决策依据稳定 |
| 5 | 编辑器 rollback | pin 释放 |
| 6 | vacuum 再请求到当前头 | `removed=1`，序号 2 的查询从此 410 |

**优点（有观测支撑）**

- 决策依据恒定：同一事务内重复读与 read-your-writes 都落在同一快照，`plan.snapshot` 始终等于 begin 序号；
- 对后台清理免疫：只要事务活着，任何 `at < 其快照` 的版本都不会被回收，编辑器不会遇到中途 410；
- 适合“先读一组数据、人工或多步计算、再整体提交”的交互编辑，以及需要把多个读写动作绑定在同一读取前提上的批处理。

**成本（有观测支撑）**

- 保留成本：被 pin 期间 `stats.versions` 不下降，Lab 1 中 pin 时为 3，关闭并清理后才降到 2；链越长、会话越久，内存累积越多；
- 写冲突概率随时间上升：提交时对写集中每个键检查 `store.Latest(table,key) > tx.Snapshot`（`engine/transaction.go:169`）。持锁越久，别人改过同键的概率越高，而失败即整笔中止、事务关闭（§6）；
- 占用 128 个活动事务名额之一（`engine/transaction.go:35`），且没有自动过期，泄漏的会话会永久卡住水位（直到进程重启，重启后活动表清空）。

**适用条件**：会话边界清晰、持续时间可控（分钟级而非天级）；写集与其他会话重叠少；应用能保证 finally 式关闭（commit 或 rollback），并能在 409 后整体重开重算。

### 策略 B：短事务——每个交互结束即关闭，需要时重开；历史读用 `at`

**交错（Lab 2 的确定性时序）**

| 步骤 | 操作 | 事实 |
| --- | --- | --- |
| 1 | 短事务 b1 begin（快照 2），读到 balance=100 | |
| 2 | 写者提交 id=1→150（序号 3） | b1 仍读 100 |
| 3 | b1 rollback，b2 begin | b2 的快照是 3，读到 150——**读取依据随重开改变** |
| 4 | `at=2` 一次性查询 | 此刻仍返回 100 |
| 5 | vacuum 推到序号 3 | `removed=1` |
| 6 | 再次 `at=2` | **410，保留区间变为 3..4** |

**优点**

- 不长期占 pin，版本链可以按清理计划及时收缩，内存与索引候选规模可控；
- 不占活动事务名额，不存在会话泄漏锁死水位的问题；
- 同键冲突窗口短，first-committer-wins 失败的概率和返工成本低。

**成本与陷阱**

- **读取依据会在重开时改变**：b1 与 b2 看到的是两个不同序号的数据库，任何“基于上一轮读数做这一轮决策”的逻辑都必须显式重读，不能假设世界没变；
- **`at` 查询不提供任何保护**：它不注册活动事务，vacuum 推进后旧序号直接 410。需要反复回看某一历史序号时，`at` 只能在该序号仍处于保留区间内使用；要稳定持有历史视图，只能开一个事务去 pin 它；
- 目录也是快照的一部分：在序号 4 开启的事务看不到序号 5 才创建的关系——查询返回 410 `relation did not exist at snapshot`，对其 `put` 返回 409 `relation outside transaction catalog snapshot`（Lab 2 实测，对应 `engine/transaction.go:72` 与 `engine/read.go:35`）。短事务反复重开意味着总能看到新关系，长事务则在其生命周期内被冻结在旧目录上。

**适用条件**：每个交互都是独立的小工作单元；应用不依赖跨请求的稳定读取前提，或愿意在每个新事务里重新建立前提；历史回看属于“尽力而为”，能处理 410 并回退到当前读。

### 3.1 并排对照

| 维度 | 策略 A 长事务 | 策略 B 短事务 + at |
| --- | --- | --- |
| 重复读稳定性 | 事务生命期内恒定（实测 `plan.snapshot` 不变） | 仅单事务内稳定；重开即跳到新序号 |
| 对 vacuum 的影响 | 把实际水位压到自身快照，`pinned=true` | 无 pin；vacuum 可推进到当前头 |
| 历史序号访问 | 事务活着就不会被回收 | 不防回收，超界即 410 |
| 版本/内存保留成本 | 随会话时长与链长度累积 | 低，清理后 `versions` 立即下降 |
| 同键 409 概率 | 随持有时长上升 | 窗口短，概率低 |
| 活动表/名额风险 | 128 名额、无自动过期，泄漏会永久卡水位 | 几乎不持有 |
| 目录可见性 | 冻结在 begin 时的关系集合 | 每次重开看到最新目录 |
| 失败返工代价 | 整笔私有写集作废，需从头重开 | 单交互范围小，返工局部化 |
| 典型场景 | 多步交互编辑、单会话内一致性决策、必须防 410 的历史分析 | 高频小写入、点对点查询、可接受历史窗口滑动的后台任务 |

实务上通常是**混合策略**：交互式编辑会话用 A 并强制有界寿命与 finally 关闭；后台清理与无状态 worker 用 B，把 `at` 当成可能失效的缓存键而非权利。需要长时间分析固定数据集时，先评估保留成本，必要时用 `GET /backup` 取冻结图像到独立还原实例（README「日志、检查点、还原」），而不是在生产库上挂一个天数级的长事务。

---

## 4. 版本保留的精确规则：为什么“当前值已更新”不等于旧版本可删

回收逻辑在 `versions.Store.Vacuum`（`internal/versions/vacuum.go:10`），对每条版本链做两件事：

1. **保留水位时刻的最后版本和所有更新版本**：循环找到最大的 `keep` 使 `chain[keep+1].Sequence <= horizon`，删除 `chain[0..keep)`，留下 `chain[keep:]`。也就是说水位处那条“当时仍可见”的版本必须留下，即使它后面已经有更新的版本；
2. **水位前的最终墓碑可以整条移除**：若链上最后一个版本 `Sequence <= horizon` 且是墓碑，整个 key 删除。

Lab 5 用一条四版本链验证了这条规则。id=1 在序号 2/3/4/5 各有一个版本（100→110→120→130），id=2 在序号 2 有行、序号 6 有墓碑；在 horizon=4 清理：

```text
vacuum-horizon-4 → {"horizon": 4, "removed": 2, "retained": 4, "pinned": false}
read-at-horizon-4 → id=1 balance=120（水位处版本保留，可读）
read-below-horizon-3 → 410 snapshot unavailable; retained interval 4..7
```

随后越过墓碑清理到序号 6：

```text
vacuum-past-tombstone → {"horizon": 6, "removed": 3, "retained": 1}
stats → keys=1, versions=1   （id=2 整条链连同墓碑消失，id=1 只剩序号 5 的版本）
```

同时还有两条硬约束（实测）：

- 水位不能回退：请求小于当前 horizon 返回 400 `vacuum horizon outside retained interval`（`engine/maintenance.go:79`）；
- 水位不能越过活动事务：Lab 5 中只读事务在序号 8 开启、之后出现序号 9 的提交，请求推到 9 时实际水位被压到 8，`pinned=true, removed=0`；关闭该只读后再清理才到 10。

因此调用方不能做的推理是：“我已经读到 130 了，100/110/120 这些旧值没人要了”。只要还有快照落在旧序号上的活动事务，或者某个旧版本是当前水位处的最后可见版本，它就必须保留。删除决策完全归引擎，调用方负责的是**通过关闭事务释放 pin、通过选择 horizon 表达容忍的历史窗口**。

---

## 5. 同键冲突保护的确切边界：禁止扩大解释

### 5.1 引擎实际做了什么

提交路径（`engine/transaction.go:155`）在全局锁内依次做三件事：

1. **同键 first-committer-wins**：对写集每个键，若 `store.Latest(table, key) > tx.Snapshot`（链尾序号比自己的快照新，含别人对该键的删除墓碑），返回 409 `write conflict on <relation>`；
2. **唯一索引最终态校验**：`d.unique` 扫描**当前序号**的整表并叠加本事务写集，对受影响关系上的每个唯一索引检查重复（`engine/constraints.go:10`）。多个 null 允许共存；同一事务内部可以互换唯一值（Lab 4 的 in-txn swap 成功，序号 6）；
3. 校验通过后才 `publish`：完整帧写盘、fsync、再内存应用（README 提交顺序；`engine/database.go:99`）。

### 5.2 这些保护不覆盖什么

- **只保护写集中实际出现的键**。两个事务写互不相交的键时，第 1 步对双方都不触发，无论它们的决策读是否互相依赖。
- **唯一索引是面向“提交后最终状态”的逐行值约束，不是谓词锁**。它能拦住两个不同主键抢同一个非 null 唯一值（Lab 4：u2 提交返回 409 `unique constraint by_note violated`），但不知道你的 `WHERE` 谓词覆盖哪些行，不防幻读，也不理解跨行算术条件（如 Lab 3 的“余额总和不得低于 100”）。
- **全局锁序列化的是请求处理段，不是事务**。begin、每次 put、每次 query、commit 各自拿锁放锁；两个事务的交错在锁外发生。因此“有全局锁所以等价串行执行”是错误推理。

### 5.3 写偏差实测：两个不相交键写者双双提交（Lab 3）

应用不变量（仅存在于调用方逻辑中）：**总余额任何时候都不得低于 100**。初始两行各 100，总额 200；每个会话从自己负责的账户取走 100，提交前用快照验算 `total - 100 = 100 ≥ 100`，认为安全。

```text
t1 决策读（快照2）: total=200 → 验算 200-100=100，合规 → 向 id=1 写入 balance=0
t2 决策读（快照2）: total=200 → 验算 200-100=100，合规 → 向 id=2 写入 balance=0
t1-commit                     → 200 sequence=3
t2-commit-disjoint-key        → 200 sequence=4   （无冲突）
final-state                   → 两行均为 0，total=0，违反总额 >= 100 的不变量
```

两个事务在任何串行顺序下都不可能双双成功：若 t1 先执行，t2 的决策读会看到 total=100，验算 `100-100=0 < 100`，按不变量必须中止；反之亦然。但 SI + 同键 FCW 合法地放行它，因为双方的写键互不相交、提交期的同键检查对谁都不触发，引擎里也没有任何组件登记“总额 ≥ 100”这个谓词。**凡是跨行、跨关系的谓词级不变量，都必须由调用方用自己的手段维护**，可选做法：

- 把冲突收敛到同一个热点键：让双方都写同一“账本/门闩”行，把写偏差降级为同键 FCW（输家拿 409 后重开重算）；
- 在应用层对受影响资源加外部串行化约定（单 worker、分区串行、advisory 协议）；
- 接受 SI 语义并把约束改成可以异步审计的形式，用 `/changes` 流事后核对（`engine/changes.go:13`，变更按提交序号分页、不拆事务）。

不要指望唯一索引替你做这件事——它回答的是“两列值是否重复”，不是“一组行是否满足谓词”。

---

## 6. 提交、失败与事务生命周期的事实清单

以下每条都可在 Lab 4 与 walkthrough `txn-conflict` 中复现：

1. **失败提交不发布任何私有行，也不消耗序号**。Lab 4：t1 提交后头序号为 4，t2 同键 409 后头序号仍是 4（`failure_consumed_sequence=false`），t2 写集中附带的 id=9 此后查询为空。
2. **失败即关闭，不可重试原事务**。对已失败事务再次 commit 返回 404 `transaction not active`。正确做法是开新事务、重读、重建写集。rollback 同样移除事务并释放 pin（walkthrough `transaction-close` 断言 `stats.active` 清空）。
3. **只读事务拒绝写入**（409 `read-only transaction`，`engine/transaction.go:65`），但仍然 pin 水位；适合纯分析会话，成本与读写事务相同。
4. **空提交不产生记录**：写集为空时直接返回 `Sequence: tx.Snapshot`，不走 publish（`engine/transaction.go:180`），不推进序号。
5. **`insert_only` 的存在性判断基于事务快照叠加私有写集**（`engine/transaction.go:80-86`），因此它防的是“在我看来该键已存在”，不是全局串行的插入门闩；两个同快照事务 insert 同一键时仍由提交期 FCW 决出唯一赢家。
6. **容量边界**：活动事务 128、每事务写集 1000 键（超限 429）、请求体 1MiB、单帧 16MiB；超限拒绝不改变已发布状态。
7. **DDL 不在用户事务内**。建关系/建索引各自是一条独立耐久记录；关系的 `Created` 序号决定旧事务能否访问它（§3 策略 B 的 410/409 观测）。
8. **存储不确定性是 fail-closed**。写盘或 fsync 失败后 WAL 进入 `Poisoned`，实例拒绝一切后续请求（503，`engine/database.go:80`、`journal/wal.go:168`）。调用方可能无法判断那次提交是否落盘，基线没有请求级幂等协议，必须重启后用实际状态核对，再决定是否补写。
9. **进程重启清空活动事务表**（`engine/database.go:96`），因此 pin 不跨重启；但已发布的回收水位经检查点/日志重放恢复，旧序号在重启后依旧 410（Lab 1 重启后 `horizon=4` 保持，§8）。

错误码对照（`transport/server.go:54`）：400 无效输入、404 事务/行不存在、409 写冲突或唯一冲突、410 快照/变更历史不可用、429 容量、503 存储不确定或已关闭。

---

## 7. 隔离级别声明：源码到底承诺了什么

| 性质 | 是否提供 | 证据 |
| --- | --- | --- |
| 事务内重复读、读己所写 | 是 | 固定 `tx.Snapshot` + 写集 overlay，`engine/read.go:28,55` |
| 提交原子性（全有或全无） | 是 | 失败先于 publish，私有写集随事务删除，§6 第 1 条 |
| 同键 lost update 防护 | 是 | first-committer-wins，`engine/transaction.go:169` |
| 唯一列最终态约束（多 null 除外） | 是 | 提交期整表校验，`engine/constraints.go:22` |
| 谓词锁/幻读防护 | **否** | 无任何谓词登记结构；唯一校验只看等值重复 |
| 跨行/跨关系不变量（写偏差防护） | **否** | Lab 3 双提交实测 |
| 可串行化 | **否** | README 与 PROJECT_SPEC「关键约束」明确不承诺；全局锁仅覆盖单次请求 |
| 历史快照读取 | 是，限于保留区间 | `at` + `Visible`，越界 410 |

一句话边界：**LaminaDB 保证“你在自己的快照里读到的东西不会变，别人和你改同一个键时只有一个赢，唯一列不会出现重复非 null 值”；不保证“你的多步决策相对于别人的多步决策存在某个串行顺序”。**

---

## 8. 清理与日志、检查点、备份的关系（成本核算依据）

调用方估算保留成本时必须区分三层存储：

1. **内存版本链（vacuum 能压缩）**：`stats.versions`、`stats.keys`。vacuum 后立即下降，Lab 5 从 6 个版本降到 4 再到 1。
2. **WAL 日志磁盘（vacuum 不压缩）**：完整日志一直保留，检查点不截短日志（README「日志、检查点、还原」）。Lab 1 中两次 vacuum 后 `journal_records` 反而从 4 增到 5——每次 vacuum 自己也是一条记录。磁盘增长只能靠外部归档/重建手段处理，不要把 vacuum 当成磁盘回收。
3. **索引候选（随历史超集增长，vacuum 后重建）**：索引树保留历史非空值到主键的候选（`index/registry.go:10` 遍历整条链 Add），vacuum 后 `index.Build` 全量重建（`engine/maintenance.go:94`）。长事务 pin 住旧版本期间，候选也同步膨胀，表现为查询 `plan.candidates` 与 `scanned` 差距拉大；候选只是线索，结果始终回版本链按快照取真实行（`engine/read.go:48`，walkthrough `query-plan` 证明索引与扫描结果一致）。

持久性与恢复相关的事实：

- vacuum 记录走与提交相同的 publish 流程（先耐久后应用，`engine/maintenance.go:90`），重放时校验水位单调（`engine/replay.go:58`）；
- `POST /checkpoint` 在锁内捕获含 `horizon` 的一致图像（`engine/maintenance.go:13`），会暂停提交和查询；Lab 1 重启后 `horizon=4` 与回收前一致，序号 2 仍 410——**回收是持久事实，不是会话期缓存**；
- `GET /backup` 冻结当时目录、保留版本与水位，还原实例保留图像内历史读版本，但日志从恢复基点重新开始，更早的 `/changes` 游标返回 410（walkthrough `backup-restore`）。需要长期持有某一历史视图又不想 pin 生产库时，这是推荐的隔离手段。

---

## 9. 复现实验指引

脚本只依赖 Python 3 标准库和公开 HTTP 接口，为每个 Lab 建独立临时实例并在退出时清理：

```sh
# 先构建与当前平台匹配的二进制（仓库自带 bin/laminad 是 macOS arm64）
go build -o bin/laminad-local ./cmd/laminad
python3 -I examples/txn_strategy_lab.py --binary bin/laminad-local            # 全部 5 个 Lab
python3 -I examples/txn_strategy_lab.py --binary bin/laminad-local --lab 3    # 只跑写偏差
```

| Lab | 对应章节 | 演示的判定 |
| --- | --- | --- |
| 1 long-lived-txn | §2.3、§3 策略 A、§8 | pin、可重复读、关闭后回收与 410、重启后水位持久 |
| 2 reopen-basis | §3 策略 B | 重开改变读取依据；at 不防回收；旧事务访问新关系的 410/409 |
| 3 write-skew | §5.3 | 不相交键双提交违反跨行总额不变量，SI 非串行化 |
| 4 fcw-and-unique | §5、§6 | 同键 FCW、失败不耗序号且事务关闭、唯一最终态校验、事务内换值 |
| 5 retention-shape | §4 | 链保留规则、墓碑整条移除、水位单调、只读事务 pin |

每行输出形如 `{"lab","step","http","obs"}`，本指南引用的返回体均取自该脚本的真实运行结果；序号在给定交错下确定性可复现（种子恒为序号 2，后续序号按表逐次 +1）。基线功能未被该脚本改动，`examples/walkthrough.py --workflow all` 的 14 条工作流仍全部通过。

---

## 10. 责任边界总表：源码保证 / 实际事实 / 调用方约束

| 主题 | 源码保证（引擎责任） | 可核对的实际事实 | 调用方仍需承担的约束 |
| --- | --- | --- | --- |
| 快照读取 | 事务固定 begin 序号，`Visible` 按链二分取版本 | 事务查询 `plan.snapshot` 恒定；普通读随头序号变化 | 普通读/重开事务不具跨请求稳定性，需要前提就显式开事务 |
| 历史保留 | 水位取请求值与活动快照最小值；保留水位处最后版本 | `pinned=true` 时 `removed=0`；关事务后同请求生效 | 及时关闭事务释放 pin；不要假设旧值随当前值更新即可删 |
| 历史访问 | 仅服务 `[horizon, sequence]` 且关系已存在的序号 | 越界 410；`at` 查询在 vacuum 后失效 | `at` 要处理 410 回退；长期历史视图改用事务 pin 或 backup 还原 |
| 同键并发 | first-committer-wins，失败不发布、不耗序号、关闭事务 | 409 后头序号不变、附带写入不可见、再提交 404 | 409 后开新事务重读重建；不要复用失败事务 |
| 唯一约束 | 提交期对最终可见状态校验，多 null 允许，事务内可换值 | 异键同值 409；同事务 swap 200 | 唯一索引不是谓词锁，不防幻读与跨行规则 |
| 跨行不变量 | **无**串行化、无写偏差防护 | Lab 3 两个 disjoint 写者均成功 | 热点门闩行、外部串行化或异步审计由应用实现 |
| 事务寿命 | 无自动过期；128 名额、1000 写集上限 | 泄漏事务持续出现在 `stats.active` 并压住 horizon | 有界会话 + finally 关闭；监控 `stats` 的 active/versions |
| 只读会话 | 拒绝写入但同等 pin 水位 | 只读事务期间 vacuum 被压回其快照 | 分析会话同样要按时关闭 |
| 耐久与恢复 | 先 fsync 后发布；失败 fail-closed；水位随检查点持久 | 中毒后 503；重启后 horizon 与 410 保持 | 不确定提交重启核验；无幂等协议，补写前先核对 |
| 容量成本 | vacuum 只压缩内存链与索引；日志不截短 | `journal_records` 只增；`versions` 随 vacuum 下降 | 磁盘增长另行规划；长 pin 的内存/候选成本纳入容量评估 |

---

## 附：关键源码索引

- 事务 begin/put/delete/commit/rollback：`internal/engine/transaction.go`
- 快照选择、区间与关系年龄校验、read-your-writes：`internal/engine/read.go`
- 版本链可见性与回收算法：`internal/versions/store.go`、`internal/versions/vacuum.go`
- vacuum 水位钳制、检查点、备份、还原：`internal/engine/maintenance.go`
- 唯一约束最终态校验：`internal/engine/constraints.go`
- 发布顺序与重放校验：`internal/engine/database.go`、`internal/engine/replay.go`
- WAL 帧、fsync 与中毒语义：`internal/journal/wal.go`
- 索引历史候选与重建：`internal/index/tree.go`、`internal/index/registry.go`
- HTTP 错误码翻译：`internal/transport/server.go`
