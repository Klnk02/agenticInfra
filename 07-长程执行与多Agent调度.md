# 长程运行与多 Agent：一次运行有两套调度器

任务调度器看 run、DAG 依赖、租户、worker、等待和截止时间；推理调度器看请求、token 预算、KV 页和 batch。**一个 run 可以等待几小时却不应一直占着 worker；一个已就绪的节点也不等于模型引擎有页可接它。** 下文刻意分开两个层次，不靠安排角色对话来模拟“多 Agent”。

## 一｜Durable Execution 和长程运行解决不同的故障

- **Durable Execution** 回答进程不在了，从哪个事实继续。重跑[两个提交域实验](02-首课与实验手册.md)：把已提交的模型提案与外部操作绑定为持久意图。显示“派发过”并不能决定是否重试。进一步看[LangGraph 同一并行步的部分成功](03-开源源码导读.md)。
- **长程运行**多问三个问题：等待期间用什么唤醒？几万条事件的恢复成本如何有界？跨版本恢复时谁仍能解释旧历史？稳定键解决不了这些问题。

```powershell
python 实验/long_run.py
```

[演示](实验/long_run.py)生成 10,000 条确定性增量和周期快照，以新 SQLite 连接恢复。对照从头求和，状态均为 9998；间隔 256 时尾部重放 16 条，间隔 1024 时重放 784 条。指标是**重新应用事件数**，不是磁盘读取时间；为了验证，脚本还会全扫一遍作裁判。初始数据批量写入，不模拟每一步的崩溃或 fsync。写快照频率与恢复工作量的取舍是真题，不要从两组尾数推出“短间隔总是更快”。

**设计练习（不冒充实测）：** 给 run 写下 `waiting_until/deadline/cancel_requested/owner_epoch`；把定时器或工具完成通知持久化，允许重复唤醒但不重复接纳同一 `event_id`。模拟 `W1 丢租约 → W2 接管 → W1 带旧回执返回`，旧 epoch 不得推进状态，外部事实仍要对账。做一张升级表：旧历史能否由新 reducer 解码、失败时能否回退、如何滚动到新 run、哪些去重键/待处理意图必须继承。阅读 Temporal Go SDK 的 [Continue-As-New](https://github.com/temporalio/sdk-go/blob/0353a6d6a68ddea841e88b7c7e98c7570dca2040/workflow/workflow.go#L750-L773) 与 [Activity 心跳](https://github.com/temporalio/sdk-go/blob/0353a6d6a68ddea841e88b7c7e98c7570dca2040/activity/activity.go#L61-L78)：换新的运行历史、保存状态快照、报告 Activity 进展不是一个动作。真系统需说明待处理消息的交接与数据保留策略；SQLite 演示没有启动 Temporal 服务。

## 二｜多 Agent 调度：同一依赖图，同时服务两个租户

[调度器](实验/dag_scheduler.py)让 A 的 plan 扇出多个独立执行节点，全部成功才可 join；B 稍晚提交另一条 DAG。两个 worker，推理每 tick 最多服务两个模型工作单位。A 的分支中途等外部工具 4 ticks。工具完成时间预定；不模拟工具池、网络、GPU 内核或真实 token 速率。队列最终跑完不等于达成服务目标。

```powershell
python 实验/dag_scheduler.py
python 实验/dag_scheduler.py --burst
```

先只改等待策略：`hold` 是等工具也占 worker；`park` 是保存待唤醒节点，空出 worker。再保持 `park` 只改准入顺序：全局 FIFO → 就绪租户轮转（租户内 FIFO）。在默认 3 分支的负载中，第二次改变没有改善 B 的延迟；换 8 分支突发再测：

| 突发时的策略 | 被等待占住的 worker·ticks | B 从到达到 join 的 ticks | 全部完工 ticks | 模型工作单位总量 |
|---|---:|---:|---:|---:|
| 占住 worker + FIFO | 32 | 31 | 33 | 30 |
| 等待让位 + FIFO | 0 | 13 | 16 | 30 |
| 等待让位 + 按租户轮转 | 0 | 5 | 17 | 30 |

第三组没有减少模型计算，也不是“整体更快”；它用一个 tick 的工期换 B 少等八个 ticks。这里的公平是按**准入机会**轮转，不是按 GPU 时间、token 或成本公平。选一个 SLO，例如 B 延迟 ≤8 ticks、整批 ≤18 ticks，再决定策略。只有一个 B 样本应报告其延迟，不能叫 P95。

**自己改变一个变量：** 将 `model_budget` 从 2 改为 1；验证 `peak_model_batch ≤ 1`、`max_owners ≤ 2`、join 开始不早于最慢父节点完成。若 fan-out 无界，worker 限额只约束 running，不约束待提交对象数。读 Ray 的 [提交端限制 pending](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/doc/source/ray-core/doc_code/limit_pending_tasks.py) 与 [声明 running 资源需求](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/doc/source/ray-core/doc_code/limit_running_tasks.py)，再到 [raylet 的派发入口](https://github.com/ray-project/ray/blob/03491225d59a1ffde99c3628969ccf456be13efd/src/ray/raylet/scheduling/cluster_task_manager.cc#L200)。课程 RR 是自定义对照，不是 Ray 默认调度策略。

**生产边界：** 准入还要考虑 KV 容量/推理队列反馈、每租户 pending 上限、超时取消与重试预算。parked 节点同样消耗持久存储和定时器容量，不能当作零成本。一个父节点取消后，要定义已派发子任务还能否写、late result 到哪、join 是 all/any/quorum；不要把“取消了父 run”硬解释为“下游动作没发生”。
