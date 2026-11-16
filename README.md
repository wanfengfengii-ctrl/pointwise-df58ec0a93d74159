# 离线读数窗口数仓

Python 3.11 标准库项目。把一日读数作为不可变批次接纳，经外存排序和窗口加工，原子发布按原输入顺序排列的逐行金额。SQLite 保存批次、尝试回执与历史发布；运行段和加工候选只属于当次作业。

## 运行

在项目根目录执行，无需安装依赖：

~~~sh
python3 -B -m unittest discover -s tests -v
python3 -B -m warehouse demo
~~~

demo 读取 samples/readings.jsonl，在专用临时目录真实接纳、加工、制造一次发布前失败并重跑，输出金额和回执，结束后删除演示数据库。回归也只使用各自的临时目录。

## Python API

~~~python
from warehouse import Warehouse

with Warehouse("readings-data") as w:
    w.ingest("day-01", [
        {"row_id": "a", "partition": "north", "order_key": 10, "amount": 3},
        {"row_id": "b", "partition": "north", "order_key": 10, "amount": 8},
        {"row_id": "c", "partition": "north", "order_key": 11, "amount": 5},
    ])
    receipt = w.compute("day-01", 1, 4096, fan_in=2, delivery_rows=1)
    assert w.read("day-01") == [
        {"row_id": "a", "window_sum": 8},
        {"row_id": "b", "window_sum": 13},
        {"row_id": "c", "window_sum": 16},
    ]
    state = w.status("day-01")
~~~

Warehouse(root) 自动创建目录和版本为 1 的 SQLite 目录库。一个根目录同时只运行一个工作者；不支持回调重入写入，也不承诺并发加工或进程终止后的续算。对象是上下文管理器，也可显式 close。重新打开后可读取已提交批次、历史回执和当前成果。

* ingest(batch_id, rows) 流式接纳有限 iterable。每项必须是恰含 row_id、partition、order_key、amount 的 dict。row_id 和 partition 是非空合法 UTF-8 字符串，各至多 256 字节；batch_id 至多 128 字节。order_key 与 amount 必须是 Python int，拒绝 bool、浮点数和缺失字段。它们没有 64 位范围限制。
* 同批 row_id 唯一，重复业务值仍是不同读数。接纳保持枚举次序；接纳后不可改写。同 ID、同内容且同次序重试返回相同的 batch_id、digest、rows 回执；不同内容或次序抛 BatchConflict。校验错误和来源迭代异常整批回滚，不留部分输入。摘要之外还比较规范行内容。
* compute(batch_id, radius, memory_budget, *, fan_in=4, delivery_rows=64, event_port=None) 为一个已接纳批次生成候选并替换完整成果。radius 是任意精度非负 int；其余数值参数也拒绝 bool。fan_in 为 2..32，delivery_rows 为 1..10000。memory_budget 的范围为 1024..2**63-1，且须满足下述逐批最低预算。返回 batch_id、publication_id、attempt_id、radius、rows、stats。
* 当前行 r 的范围是同分区且 order_key 位于 [r.order_key-radius, r.order_key+radius] 的全部行。EXCLUDE TIES 移除与 r 同键的其他行，保留 r 自身。金额使用 Python 整数精确累计；radius=0 的结果恰为各行自身金额。
* read(batch_id) 返回当前成果的 list，每项是 row_id、window_sum，次序等于原输入次序。iter_read(batch_id) 返回同一成果的逐行迭代器，提前停止须 close；关闭迭代器后再写入或关闭 Warehouse。read 是便利整值接口，其返回列表不计入加工内存预算。
* status(batch_id) 返回接纳行数、摘要、最大规范行字节数、当前发布 ID、当前 publication（含 radius、rows、stats）及 latest_attempt。最近尝试可能失败，此时当前发布仍是原成功版本。
* 未知批次抛 NotFound；已接纳但尚未发布抛 NotPublished；字段和参数错误抛 ValidationError。这些类继承 WarehouseError，带稳定 code：BATCH_NOT_FOUND、NOT_PUBLISHED、INVALID_INPUT、BATCH_CONFLICT。来源与生命周期端口抛出的异常原样传播。参数不合格不会创建加工尝试。

接纳实现见 warehouse/intake.py；排序见 warehouse/sort.py；窗口见 warehouse/window.py；目录事务与读取见 warehouse/catalog.py；公开装配在 warehouse/__init__.py。

## 外存与预算

输入键和金额在磁盘中以规范带符号十六进制 TEXT 保存，绕过 SQLite 64 位整数和 Python 十进制字符串位数上限。数值索引使用精确 HEXINT 比较；绝不使用 SQLite SUM 或浮点转换。

令 M 为 status 的 max_payload：规范行 [原序号,row_id,partition,hex(order_key),hex(amount)] 的 ASCII JSON 加换行字节数最大值。每次加工最低预算为 max(1024, (fan_in+4)*M)。超过单行最低需求时参数检查直接拒绝；不会把超大单行偷偷当作小行计量。

memory_budget 约束算法持有的规范输入行载荷，按上述字节口径计算。peak_payload 是保守的行载荷预留高水位：排序块另留输入、编码和交接余量；归并另留三个最大行槽；交付批另留一个槽。它不是 RSS 或精确分配器测量。Python 对象头、文件缓冲、固定数目的键/计数器/任意精度累加器与单个输出值不按输入行载荷收费；其数量不随分区或同值组行数增长。每个 SQLite schema 的页缓存设为 1024 KiB，temp_store=FILE；归并文件缓冲和描述符数量仅随 fan_in 增长。回归另以 tracemalloc 对固定预算、增大行数的加工做真实 Python 分配量检查。

排序先生成有限载荷运行段，再以磁盘清单逐轮、有限扇入归并。所有行携带原序号和 row_id；运行段或交付批结束没有语义含义。输入、单个同值组、单分区都可超过预算。

窗口先把所有有序行完整暂存磁盘，之后逐行查询闭区间并以 Python 整数累计。该实现正确、内存有界，但稠密范围可能访问平方级邻行；stats.range_rows_visited 如实累计访问数。它适合离线核账，不能据此假定线性运行时间。stats 还包含 initial_runs、merge_passes、sort_bytes_written（含中间段累计真实文件长度）、staged_rows、staged_bytes 和 candidate_bytes（对应真实 SQLite 文件长度）。磁盘无固定容量上限，需由调用方提供空间。

## 发布与失败

每次成功接纳和每次成果发布都使用独立 SQLite 事务。加工暂存对 read 不可见；发布逐条校验原序号、row_id 和完整行数，再拷入候选发布。在切换 head 前关闭候选并清理本次工作目录，随后同事务提交 head 和成功回执。旧发布保留为审计，不自动回收。

event_port(event, details) 是可选同步生命周期端口，返回值忽略。sort_run 在运行段文件真实写完关闭后触发，携带 path、rows；window_staged 在完整有序暂存提交后触发，携带 rows、bytes；window_complete 在完整窗口结果提交后触发；publish_ready 在结果已复制入目录事务、工作目录已释放、head 尚未切换时触发。端口可读取状态或做审计，不可重入写入。测试的失败端口在这些真实边界抛 OSError，不替换存储或排序实现。

执行异常回滚候选发布，记录失败尝试并清理当次暂存，已发布成果保持可读。正常重跑创建新的尝试并可完整替换。此保证针对可捕获的加工/端口/存储操作异常；操作系统强杀及目录库自身无法写回时不提供恢复协议。不得手工修改目录表、运行段或 SQLite 排序规则。

## 原业务回归

tests/test_business.py 覆盖多分区、负键、正负金额、空批次、替换和重开；test_intake.py 覆盖重试、重复身份、冲突、校验和来源失败；test_window.py 覆盖同键排除、超过十进制默认位数限制的整数、闭区间和确定性关系差分；test_external.py 覆盖超预算同值组、真实磁盘文件、切批/扇入不变性及固定预算分配量；test_failures.py 覆盖已写运行段、窗口暂存和发布前失败后的保全、清理及重跑。关系期望独立按公开定义枚举，断言失配标记为 WINDOW_ROW_MISMATCH。
