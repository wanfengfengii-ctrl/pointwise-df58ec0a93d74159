# 派生批次交接说明（derive / refresh）

本说明记录 DERIVED.md 两项能力的实现不变量、可核对的局部工作与载荷证据，以及失败边界。
业务定义、窗口语义与异常码仍以 README.md、DERIVED.md 为准；原接口与原 tests/ 断言未修改。

## 代码落点

- `warehouse/derived.py`：`derive(...)` 流式接纳与 `refresh(...)` 局部刷新，全部新逻辑集中于此。
- `warehouse/catalog.py`：新增 `derivations`（固定绑定）、`derived_changes`（规范更正序列）两张表，
  以幂等 `CREATE TABLE IF NOT EXISTS` 在开库时补齐，**`PRAGMA user_version` 仍为 1**。
  可复用父范围索引 `parent_range ON inputs(batch_id,part,k COLLATE HEXINT,seq)`
  不在建库时创建，而由首次 refresh 建立并持久复用。
- `warehouse/__init__.py`：新增 `Warehouse.derive`、`Warehouse.refresh`；
  `ingest` 拒绝覆盖派生批次，`compute` 拒绝派生批次，`refresh` 拒绝普通批次。
- `tests/test_derived.py`：同契约补充测试（不改原 tests/ 与 derived_check.py）。

## 关键不变量

1. **固定绑定**：derive 记录确切 `parent_publication_id` 与该发布的 radius（hex 文本）。
   父批次之后 `compute` 换 radius 只改父 head，派生刷新始终读绑定发布的 inputs/outputs。
   绑定发布允许不是父当前 head；派生发布作为父发布一律 `ValidationError`，不支持派生链。
2. **同一旧像核对**：所有 before 都以同一父输入做集合核对（SQL JOIN/EXISTS），
   不按前一项应用后的状态核对；旧像不符、身份缺失、新身份未给 `before=None`、
   两侧皆 None、重复 row_id、非法字段均 `ValidationError`，来源迭代异常原样传播，
   整批回滚，不留下 child 批次、绑定或临时表。
3. **次序与身份**：存续行按父 `seq` 保序，新增行按更正 `change_seq` 追加；
   子输入用新的连续序号（`row_number() OVER (ORDER BY position)-1`），稳定身份仍是 row_id。
   digest 用原 `model.pack` 按新序号逐行 SHA256。重试幂等同时比较 digest、行数、
   完整规范更正序列（双向 EXCEPT，含次序）与完整子输入；任一不同即 `BatchConflict`。
4. **局部集合 A**：A = 更正后仍存在的被更正身份 ∪ 每个非 None before/after 位置
   （其 partition/order_key 与绑定 radius 构成的闭区间）命中的子行。
   A 外逐行直接复用绑定父发布该身份的结果（一次 reuse 事件，不重排、不查窗口）；
   A 内按闭区间 RANGE + EXCLUDE TIES 用 Python 整数重新求值（evaluate 事件，可重复）。
   删除行在求值前从 A 剔除；每个子行最终恰属 evaluate 或 reuse 之一。
5. **范围外零计算**：无关分区/无关键的行只走 reuse 路径，
   `test_unrelated_partitions_are_not_evaluated` 证明 300 个无关行零 evaluate/sort。
6. **闭区间与精确整数**：键以规范 hex TEXT + HEXINT 排序规则比较，金额全程 Python int，
   不用 SQLite SUM、不做浮点/十进制字符串转换；radius=0 结果恰为自身金额。
7. **载荷口径**：沿用 README 的规范行字节口径。refresh 最低预算
   `max(1024, 8*M)`，M = max(父 max_payload, 子 max_payload)（对应默认 fan_in=4 的公式）。
   不缓存整批输入、更正序列或同值组：更正落 SQLite 临时表，子输入全量复制到**工作目录的
   候选 SQLite**，Python 侧只保留当前行/邻行游标与固定数量累加器。
   `peak_payload` 只按规范行载荷记账，预算超限直接 AssertionError。
8. **原子发布与失败**：候选写入、head 切换、成功回执在同一目录事务；
   `publish_ready` 在候选已入事务、工作目录已释放、head 未切换时触发（details 含
   rows、publication_id）。任何异常回滚候选、记失败尝试、清理 work/ 暂存，
   旧 head/旧成果可读，重开可重试；父输入与全部父发布永不改变。
9. **事件不改变算法**：`event_port=None` 与提供端口走同一代码路径。
   另发一次 `refresh_prepared`，`copied_rows` 为完整子行复制数，
   `indexed_rows` 仅在本次首次建父索引时为父输入行数，复用持久索引时为 0。

## 可核对的局部工作证据

- 事件审计：`derived_check.Audit` 与 `tests/test_derived.Recorder` 断言
  evaluate 不越出 A、reuse 每行一次且与 evaluate 不重叠、每个子行都有归属；
  `verify` 全绿即证据。
- 索引首次建立/复用：`stats.parent_index_built`（布尔）与 `stats.indexed_rows`；
  `test_parent_index_build_once_and_reused` 同时核对 `sqlite_master` 中索引从无到有、
  第二个子批次刷新时 indexed_rows=0。
- 固定绑定换版：demo 与 `test_hand_computed_sample_and_fixed_binding` 中父先 compute(radius=1)
  再 compute(radius=0)，子成果仍为 radius=1；untouched 分区 e/f 为 11 而非父 head 的 2/9。
- 载荷证据：`test_oversized_peer_group_respects_payload_budget`（180 行大同值组，
  总载荷远超 4096 而 `peak_payload<=4096`）与
  `test_fixed_budget_working_set_flat`（tracemalloc：n=200 与 n=2000 的 Python
  峰值差 <=2 MiB、绝对值 <8 MiB）。
- 失败重开：`test_publish_failure_preserves_old_head_and_reopens` 与
  derived_check demo 均在 publish_ready 注入 OSError，核对 NotPublished/旧 head 保留、
  work 目录清空、重开后重试成功。

## 运行清单

- `python3 -B derived_check.py reference`：独立参照与手算样例（无需实现即可跑）。
- `python3 -B derived_check.py verify`：原 15 个回归 + derived_check 的 6 个派生用例
  + tests/test_derived.py 的 15 个契约用例，共 36 个。
- `python3 -B derived_check.py demo`：真实接纳、绑定、跨分区更正、父换版、
  publish_ready 失败与重开，输出真实子成果、回执、事件计数与准备计数。
- `python3 -B -m warehouse demo`：原普通批次演示保持不变。
- `.pointwise-runtime.json` 的 verify/demo 已指向上述两个夹具，格式字段不变。

## 已知边界（与契约一致，非缺陷）

- 仅普通父批次；不支持派生链、并发加工、强杀续算、历史发布回收。
- A 内邻行查询为有界内存的扫描，稠密范围可能平方级访问，`stats.range_rows_visited` 如实累计。
- 父范围索引持久建在目录库 inputs 上，仅加速查询，不改变任何输入或发布内容；
  失败的 refresh 可能已留下该索引，后续刷新直接复用。
