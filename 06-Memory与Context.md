# Memory 与 Context：先回答“现在允许看到哪一版”

**工程分工：** 运行历史是审计与恢复事实；记忆事实库跨 run 保存允许复用的信息；索引为查询生成候选；context 编译器把本轮获准可见的片段打包成模型输入；KV 是某次计算的中间态。它们没有共同的“清缓存即可修复”操作。

## 实验一：过期索引既可能读错，也可能漏读

运行 `python 实验/memory_store.py`。这是两张逻辑表，一个真实 SQLite 文件；`truth` 为权威记录、`idx` 为 FTS5 派生索引。[实现](实验/memory_store.py)故意让更新与索引刷新分离，并关闭/重开数据库。它测一致性，不宣称词法检索可以代替语义检索。

1. 租户 A、B 使用相同 `key=spec`；A 的 `route blue` 已入索引。A 更新为 `route green`，索引尚未刷新。预测三次读：只读索引 → 候选回表检查版本 → 刷新后回表。实际分别为旧值、空集、新值。**回表可以挡陈旧结果，不能召回尚未索引的新内容。** 若契约要求 read-your-writes，可以对已知 key 直读，或合并权威变更增量与索引；记录多花的读预算和去重方式。
2. A 逻辑删除，不刷新索引即查询；B 的同名记录要继续可见。再以时间 99/100 查过期边界。验收：跨租户返回数、逻辑删除泄漏数为 0；无权返回不等于物理文件已擦除。本例将 `tenant` 当作可信入口参数，不实现认证、备份清除或分片路由。
3. 换“余弦/ANN 检索”反问：向量索引能消除版本落后吗？不能。迁移设计需新增 `embedding_model_version`、双索引回填水位、切流与回滚。当先取全局 top-k 再按权限过滤时，既可能浪费召回槽位，也不能把事后过滤当成完整的租户隔离设计。明确候选域、最终授权检查和不足 k 个时的处理。

| 应量的指标 | 分母/真值 | 不要误读 |
|---|---|---|
| Recall@k | 此查询在指定时刻**获准且有效**的相关 key 集合；空真值单列 | 并非“模型回答像对的” |
| 陈旧返回率 | 返回条目中与当时业务版本不符的占比；无返回另计 | 回表后陈旧率 0 可能伴随漏读 |
| 新鲜度与读成本 | 提交到可检索的延迟；同负载的检索、回表与装包时间 | 这组小样例不支持 P99 结论 |

**源码出口：** 对照 [LangGraph Store 的 namespace、search、put](https://github.com/langchain-ai/langgraph/blob/84023451a2bd5987b1d4df530f4145d503d75ccb/libs/checkpoint/langgraph/store/base/__init__.py#L771-L887) 与 [Postgres 实现的 batch](https://github.com/langchain-ai/langgraph/blob/84023451a2bd5987b1d4df530f4145d503d75ccb/libs/checkpoint-postgres/langgraph/store/postgres/base.py#L952)。指出 namespace 和 thread checkpoint 的差别；不要假设 namespace 自动提供用户认证，也不要把课程的版本回表当作上游机制。

**存什么也属于基础设施合同。** 本轮工作集随 context 退出；可审计的经历用不可变事件与 artifact 引用保存；可被更正的长期事实则以 namespace、key、版本、来源和保留期管理。由经历推导事实时保留来源和写入幂等键，不把同一事件重放两次当成两条独立佐证。设计迁移题：两个 run 同时基于 rev 3 写 rev 4，应当条件更新后显式冲突、可交换合并，还是最后写胜出？先为具体字段选语义，再谈存储引擎；当前 SQLite 演示不是多写者冲突解决方案。

## 实验二：把 context 当编译产物，而不是提示词文案

运行 `python 实验/context_trace.py`。这是实际接起来的两段：[SQLite 检索](实验/memory_store.py) → [两个容量相同的 KV 页池](实验/kv_pool.py)。三个 turn 中持久事实分别更新为 blue、green、gold；每个 turn 两组拿到的事实相同，只改动态片段与稳定前缀的顺序。先猜两组命中的完整块数，再跑。

| 三轮合计（固定每轮 10 个教学 token） | 动态片段在首 | 稳定前缀在首 |
|---|---:|---:|
| 提供给页池的总 token 数 | 30 | 30 |
| 复用 token 数 | 0 | 16 |
| 需 prefill 的 token 数 | 30 | 14 |

**能证明：** 两组均读到当前租户的新事实，布局改变了可复用前缀。**不能证明：** 重排后的真实模型答案同样好，或 GPU TTFT 按同比例降低。不要为了命中率把过期记忆放回输入。

**本人设计题：** 给 context manifest 选字段：`run_id, step_id, template/tokenizer_version, ordered (source_id, revision, role), artifact_digest, input_budget, output_reserve`。同一已提交步骤重放应使用原输入或可验证快照，新步骤才按明示策略取新记忆。保留权限撤销优先级：历史可审计 ≠ 可以重新把已撤销的内容发给模型。给 4096 token 的总窗口、512 输出预留、1024 固定协议，动态材料最多 2560；若最新工具结果单独超过预算，先决定取引用还是显式停止，禁止无声拆开调用与结果。可复用[原消息配对测试](实验/test_harness.py)。
