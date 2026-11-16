# 派生读数与局部窗口刷新

当前仓库完整业务、原接口、编码、异常、预算口径与发布边界见 README.md。当前尚无本页的两项能力；derived_check.py 是公开核账夹具，不是业务实现。原代码、样例和全部 tests/ 文件保留。本页所有新要求属于公开交付契约。

## 共同结果和调用

接纳一个固定历史发布上的更正批次，再以同一绑定刷新其完整成果。只接纳更正却继承全部旧结果会留下错误邻行；刷新若读取父批次当前 head，又会混用不同 radius 的成果。

新增：
- Warehouse.derive(child_id, parent_publication_id, changes)
- Warehouse.refresh(child_id, memory_budget, event_port=None)

单工作者，关闭重开保真；不支持派生链、并发加工、强杀续算或历史回收。parent_publication_id 必须是当前目录中普通 ingest 批次的确切成功发布，允许不是当前 head；必须是非布尔正整数。未知发布抛 NotFound，派生发布作为父发布抛 ValidationError。固定的是该发布的输入、成果和 radius；之后父批次 compute 不改变绑定。

 derive 返回恰含 child_id、parent_publication_id、radius、digest、rows 的 dict。child_id 沿用 batch_id 校验。digest 按子输入的新连续序号从0起，用原 model.pack 逐行拼接计算 SHA256；摘要不能代替内容比较。status(child_id) 保留旧字段，另有 parent_publication_id 和 radius 两个固定绑定字段。未刷新时 read/iter_read 抛 NotPublished；发布后沿用原输出格式和顺序。refresh 回执保留 compute 的 batch_id、publication_id、attempt_id、radius、rows、stats，并增加 parent_publication_id；允许重复刷新生成新发布。

compute 仍加工普通批次；对派生批次调用 compute、对普通批次调用 refresh 均抛 ValidationError且不开始尝试。ingest 不得覆盖派生批次。普通批次原有行为不变。

## 更正输入和身份

changes 是有限、可单遍迭代的对象序列。每项为恰含 row_id、before、after 的 dict。row_id 沿用原身份校验，同一序列内不能重复。before/after 为 None 或恰含 partition、order_key、amount 的完整 dict，非 None 像沿用原字段校验；至少一侧非 None。before==after 的无变化更正合法。

所有 before 均按同一父批次输入核对，不按前一项应用后的状态核对。None 表示该身份不存在；after=None 表示删除。已有身份必须给出相等完整 before，新身份必须给 before=None。旧像不符、重复身份或非法字段抛 ValidationError；来源迭代异常原样传播。以上失败整次不接纳，不留 child 批次或绑定。

存续行保持父输入相对次序，新增行按 changes 次序追加。结果输入使用新的连续序号，稳定业务身份仍是 row_id；删除不影响其他身份。相同 child_id、相同 parent_publication_id 及相同规范更正序列重试幂等，包括更正顺序；其他组合抛 BatchConflict。空序列合法。同一结果输入但更正序列不同仍属冲突。新 child_id 与普通批次冲突也抛 BatchConflict。derive 不加工窗口，不改变父输入或父发布。

## 局部计算与复用

沿用父 radius、闭区间 RANGE、EXCLUDE TIES 和 Python 整数精确累计。允许重新求值的集合 A 是：更正后仍存在的被更正行，加上每个非 None before/after 所指位置的同分区闭区间 [key-radius,key+radius] 中所有子批次行。位置取像中的 partition、order_key。A 允许重叠、同键邻行和结果恰好不变，不要求求出更小集合。

A 外每行必须复用固定父发布的该身份结果，不得重新排序或重新求窗口。A 内可重新求值或正确地调整父结果贡献，求值可读必要邻行。可以完整复制输入、顺序映射及成果；可以首次建立并持久复用父输入范围索引，该准备可扫描和排序全部父输入。子输入的复制、已有索引条目的维护和结果原序输出也允许全量遍历；不能把子批次的全量重新排序或窗口求值算成索引准备。没有必须采用的存储布局或算法。

沿用 README.md 的规范行载荷和 SQLite 缓存口径，不能在 derive 或 refresh 中把整批输入、更正序列或整个同值组缓存为 Python 容器。derive 无预算参数，可流式落盘当前更正；refresh 的 memory_budget 范围沿用 compute。最低预算沿用 compute 默认 fan_in=4 的公式，以父、子 max_payload 的较大者 M 计算 max(1024,8*M)。参数不合格不开始尝试。stats 至少保留 memory_budget、max_record_bytes、peak_payload、sqlite_cache_kib_per_schema 的原含义。原计量排除的固定数量整数等仍不按行载荷收费；不能通过把行改名为索引对象逃避计量。

为核对实际局部工作，refresh 沿用同步 event_port 并发出 refresh_work，details 为 {kind,row_id}。kind=evaluate 表示该行发生窗口求值或贡献更新，sort 表示该子行参与重新按窗口键排序，reuse 表示该行最终直接复用绑定父结果。所有实际 evaluate/sort 均须记录，最终未求值的每行须记录 reuse。evaluate/sort 可多次，reuse 每行一次且不得与 evaluate 重叠；每个子行最终必须属于 evaluate 或 reuse。另发一次 refresh_prepared，details 含非负整数 copied_rows、indexed_rows，分别如实累计完整行复制与索引准备/维护访问；允许为0，不把这两项计为窗口求值。这些事件是审计记录，不是用自报计数替代真实实现核验。不得因 event_port=None 而改变算法。

## 发布和失败

完整候选、当前指针和成功回执原子发布。沿用 publish_ready 边界：候选已入目录事务，工作目录已释放，head 尚未切换；details 保留 rows、publication_id。端口异常原样传播，记录失败尝试，清理暂存且不暴露半成果；已接纳子输入与绑定可重试。初次失败仍为 NotPublished；有旧子成果时保留旧 head、旧成果。父输入和全部父发布始终不变。不要求操作系统强杀恢复。

## 手算样例

父输入原序：
a=(p,10,3)，b=(p,10,8)，c=(p,11,5)，d=(q,100,7)。
radius=1 时结果为 a:8、b:13、c:16、d:7。
令 pub 为该次 compute 返回的 publication_id：
derive("child", pub, [{"row_id":"a","before":{"partition":"p","order_key":10,"amount":3},"after":{"partition":"q","order_key":100,"amount":4}}])
子输入原序仍为 a、b、c、d。即使父批次之后 compute(radius=0)，refresh("child",4096) 仍得到 a:4、b:13、c:13、d:7，radius 仍为1。q 中 a、d 同键，互相排除；b 与 a 的旧键相同，本来就没有包含 a。联合夹具另加未受影响分区的 e=(untouched,0,2)、f=(untouched,1,9)：子成果均为11，不能借用父当前 radius=0 的2、9。

## 交付核验

交付两项联动实现、简短交接不变量说明，以及可核对的局部工作和载荷证据。保留全部原接口与全部原回归。联合验证须覆盖父成果换 radius、跨分区移键、金额改变、增删、同键排除、零 radius、大整数、幂等和坏输入/来源异常，以及 publish_ready 失败后重开重试。固定更正范围并增加无关分区时，不得增加那些分区的 evaluate/sort；超预算同值组仍须满足原载荷口径。全量关系枚举是允许的独立测试参照，不是合法的生产刷新方案。

公开夹具：
- python3 -B derived_check.py reference：只核对独立参照和手算样例，当前初始树即可执行。
- python3 -B derived_check.py verify：原完整套件加派生普通回归，须在实现后通过。
- python3 -B derived_check.py demo：真实接纳、绑定、更正、父换版、发布失败和重开演示；须在实现后执行。

夹具不是穷尽验收，可补充同契约测试；不能修改原 tests/ 断言。原 .pointwise-runtime.json 已可运行现有业务，交付时将 verify/demo 接到上述联合核验和演示，保留 version=1、kind=cli、setup 为准备 argv 列表以及 verify/demo 为 argv 数组的格式。运行清单的演示必须展示真实子成果及其固定父绑定。
