# NoInterleave activation 屏蔽的残留问题与修复计划

- 日期：2026-08-22
- 相关提交：`b00ea37 解决: actor 通过 Call/Send 进入 NoInterleave 服务的路径的错乱`
- 涉及文件：`actor/system_runtime.go`、`actor/system.go`、`observe/waitgraph/`、`README.md`
- 证明用例：`actor/system_nointerleave_residual_test.go`（Part A / Part B 均已绿）

> 最终状态（2026-08-25）：activation、NoYield、完整调用环诊断和内部 invariant
> 错误均已修复。环检测采用可跨节点传播的 call path，而非全局锁 waitgraph；actor
> 内部 context 状态已收口到单一 carrier。普通 actor 的协作调用环仍保持合法。

## 一、背景：上一个修复做了什么

`serviceActivation.execute()` 用 `context.WithoutCancel(env.ctx)` 构造 handler ctx——去掉取消与超时，但**保留调用方的整条 value 链**。当调用方本身是 actor handler 时，被调服务的 ctx 里会残留**调用方的** `*serviceActivation`。

普通服务随后用 `WithValue(..., act)` 覆盖掉它，无影响；NoInterleave 分支原先「什么都不写」，于是残留值直接漏进 handler。后果：

- 调用方用 `Call` 进入 → NoInterleave handler 的嵌套 `Call` 会去**调用方的 runtime** 上发 `eventYield`，而该 activation 已挂起、不是 `current`，直接报 `actor: activation is not running`（`system_runtime.go:275`）。NoInterleave 服务无法作为 actor 调用链的中间节点。
- 调用方用 `Send` 进入 → 调用方 activation 仍在运行且是 `current`，yield **会被接受**：调用方的一次 activation 被凭空打断，下一条消息插队执行，静默破坏其原子性。

修复方式是在 NoInterleave 分支显式写入 typed-nil 屏蔽掉陌生 activation：

```go
ctx = context.WithValue(ctx, activationContextKey{}, (*serviceActivation)(nil))
```

`activationFromContext` 于是返回 nil，嵌套 `Call` 走「非 actor 调用方」路径——同步阻塞本服务直到返回，不 yield 任何人。这正是 NoInterleave 的语义。

## 二、残留问题

屏蔽的副作用是：**NoInterleave handler 在所有 activation 派生设施眼里等同于「不在 actor 内」**。由此产生三个缺口。

### Gap 1 — 外呼在 callgraph 中丢失调用方身份（功能缺口）

`Call` 在 `actor/system.go:483` 从 `activationFromContext(ctx)` 推导 `CallEvent.Caller`：

```go
caller := "<external>"
if act := activationFromContext(ctx); act != nil && act.runtime != nil && act.runtime.service != nil {
    caller = act.runtime.service.name
}
```

屏蔽后恒为 nil，NoInterleave 服务发出的**所有**边都记成 `<external>->callee`。而 `ServiceOptions.NoInterleave` 与 `README.md:132` 都明写「回调同一 service 会 deadlock 到超时」——最需要靠 callgraph 看出来的那类环，恰好就是现在看不见的边。

注：修复前这里拿到的是**残留的外层调用方**名字，同样是错的（且更具误导性）；从非 actor 入口进入时本来就是 `<external>`。所以这不是回归，而是「一直错，现在错得统一」。

失败用例：`TestNoInterleaveOutboundCallKeepsCallerIdentity`
实测：`CallEvent.Caller for leaf.ping = "<external>", want "no-interleave"`

### Gap 2 — `NoYield` 在 NoInterleave 服务内静默失效（契约缺口）

`NoYield`（`actor/system_runtime.go:44`）的注释承诺「A mailbox Call attempted from fn fails before the target request is sent」，由 `TestNoYieldRejectsCallBeforeDispatch` 断言。但它的实现是：

```go
act := activationFromContext(ctx)
if act == nil {
    return fn(ctx)   // 直通，act.noYield 不自增
}
```

在 NoInterleave 服务里 `act` 恒为 nil，护栏退化为空壳，`ErrYieldForbidden` 永不触发。典型事故：有人用 `NoYield` 包住读-改-写当护栏，后续重构往闭包里加了个 `Call`——不会快速失败，而是把整个 service 阻塞掉；若目标是可重入的，会一直卡到 `CallTimeout`。

同类降级（**属预期行为，仅需文档说明，不需修代码**）：

- `Await` 走 `act == nil` 直接执行 `wait`，「协作释放 service」失效——这正是 NoInterleave 想要的，但 `Await` 的注释写的是「called outside an actor activation」，而此处明明在 activation 内。
- `MarkTurn` / `InterleavedSince` 恒返回零值 / `false`。结论正确（不交错就不会 interleave），但调用方无法区分「不会交错」和「根本不在 actor 里」。

失败用例：`TestNoInterleaveNoYieldStillRejectsCall`
实测：`NoYield-wrapped Call inside NoInterleave = <nil>, want ErrYieldForbidden`，且目标 handler 实际被派发执行。

### Gap 3 — 自调用死锁只能靠 `CallTimeout` 兜底（能力缺口）

NoInterleave handler 回调本服务永远拿不到 turn。`observe/waitgraph` 提供了 `BeginWait`/`ErrCycle` 的环检测能力，但 **`actor/` 包一处都没引用它**（`grep -rn waitgraph actor/*.go` 为空），所以既无主动检测、也无诊断信息，只能白等一个 `CallTimeout`。与 Gap 1 叠加后，事后也无法从 callgraph 追溯。

失败用例：`TestNoInterleaveSelfCallFailsFastInsteadOfTimingOut`
实测：`actor: call completion timeout ... after 400.5ms`（等满整个 `CallTimeout`）

### Gap 4 — 结构性隐患（无测试，靠约定）

根因是 `WithoutCancel` 把调用方整条 value 链带进被调 handler。当前 actor 内部只有 `activationContextKey` 一个 key，且两个分支都做了覆盖，是安全的。但**将来新增的任何 actor 内部 ctx key 都必须在两个分支都处理**——比如为修 Gap 1 而引入 caller-name key，忘了屏蔽就会原样重演这次的 bug。

### Gap 5 — 测试覆盖

- 守住上一个修复的只有 `TestRuntimeNoInterleaveMasksCallerActivation` 一条，覆盖的是 **`Call` 进入**（会报错的那条路径）。
- **缺 `Send` 进入 NoInterleave 的用例**——那才是修复前会**静默破坏调用方串行性**的分支，危险得多。
- 缺 NoInterleave → 普通 service → 再嵌套一层的用例。
- `TestRuntimeSequentialNestedCallsResumeSameActivation` 经验证在修复前也通过，对本修复零覆盖（走的是非 NoInterleave 分支）。

## 三、修复计划

按「便宜且无争议」→「需要设计」排序。每步独立成一个 commit。

### Step 1：文档（无代码风险，先做）

1. `ServiceOptions.NoInterleave`（`actor/system.go:71`）补一段：该服务的 handler ctx 内**不携带 activation**，因此 `NoYield` 退化为直通、`Await` 不再协作让出、`MarkTurn`/`InterleavedSince` 恒为零值/false。
2. `NoYield`（`actor/system_runtime.go:44`）与 `Await` 的注释各加一句，点明在 NoInterleave 服务内的行为。
3. `README.md:132` 那段补同样的说明。

验收：文档描述与 `TestNoInterleaveNoYieldStillRejectsCall` 记录的实际行为一致（此时该测试仍红，因为 Step 3 才改行为）。

### Step 2：补齐上一个修复的测试（无代码改动）

在 `actor/system_runtime_test.go` 增加：

1. `TestRuntimeNoInterleaveMasksCallerActivationOnSend`——用 `Send` 进入 NoInterleave 服务，其 handler 嵌套 `Call`；断言**调用方 service 的 activation 未被打断**（在调用方 handler 内 `MarkTurn` + 结束前 `InterleavedSince` 应为 false）。
2. NoInterleave → 普通 service → 再嵌套一层，断言链路正常且各自 activation 归属正确。

验收：两条新测试在当前代码上**绿**；`git stash` 掉 `system_runtime.go` 的屏蔽改动后**红**（尤其第 1 条，用于固化「静默破坏」不再发生）。

### Step 3：让 activation 承载「不可让出」而非被整体抹掉（修 Gap 1 + Gap 2）

当前用 typed-nil 屏蔽属于「一刀切」：把「身份」和「可让出性」两件事一起丢了。改为**保留自身 activation，但标记其为不可让出**：

```go
// execute()：两个分支都写入自己的 activation
ctx = context.WithValue(ctx, activationContextKey{}, act)
```

并在 `serviceActivation` 上依据 `service.opts.NoInterleave` 让让出路径直接拒绝——`awaitActivationTyped` 在 NoInterleave 下不发 `eventYield`，而是直接执行 `wait`（即当前的阻塞语义）。这样：

- `Call` 拿到的是**本服务自己的** activation → `CallEvent.Caller` 恢复为真实服务名（Gap 1 解决）。
- `NoYield` 拿到非 nil activation → `act.noYield` 正常自增 → `ErrYieldForbidden` 恢复生效（Gap 2 解决）。
- `MarkTurn`/`InterleavedSince` 能区分「在 actor 内且不会交错」与「不在 actor 内」，且 `interleaveVersion` 永不递增，语义天然正确。
- 陌生调用方 activation 依然被覆盖 → 上一个修复的效果保持。

风险点，实施时必须逐一确认：

- `awaitActivationTyped` 的所有调用方（`Call`/`Await`）在 NoInterleave 下都不得触达 `eventYield`/`eventResume`；建议在 `emit` 前加一道断言或在 runtime 侧对 NoInterleave service 的 yield 事件直接返回错误，防止将来漏改。
- 确认 `noYield` 计数与「本来就不会让出」不冲突（应只影响错误返回，不影响调度）。
- 跑全量 `-race`，重点 `actor/` 的 stress 与 nointerleave contract 测试。

验收：`TestNoInterleaveOutboundCallKeepsCallerIdentity` 与 `TestNoInterleaveNoYieldStillRejectsCall` 转绿；`TestRuntimeNoInterleaveMasksCallerActivation` 及 Step 2 新增用例保持绿。

> 备选方案（若 Step 3 的改动面被评估为过大）：保留 typed-nil 屏蔽，另加一个独立的 `callerServiceContextKey` 只承载服务名，仅修 Gap 1，Gap 2 退化为纯文档说明。代价是引入第二个 actor 内部 ctx key，正好踩中 Gap 4，必须同时做 Step 5。

### Step 4：传播 call path 并检测 NoInterleave 环（修 Gap 3，已完成）

最终没有把 `observe/waitgraph` 接入 actor 热路径。每个 handler 将 `(Node, Service,
Protocol)` 追加到同步调用路径，cluster 在进程边界上传播该路径。目标为
NoInterleave 且路径中已有相同 `(Node, Service)` 时，在 mailbox admission 前返回
`ErrCallCycle`，错误携带完整闭环。这样避免了全局互斥锁，也不会拒绝普通 actor
可以通过协作让出完成的调用环。

### Step 5：收口 actor context 状态（修 Gap 4，已完成）

activation 与 call path 已合并为单一私有 context carrier。handler context 构造集中
在一个 helper：保留调用方业务 values，只显式继承 call path，并始终用当前 handler
activation 覆盖调用方内部状态。

## 四、最终验收

- [x] Part A / Part B 回归测试全部通过。
- [x] Send 进入、跨服务环、经过普通 service 的环和完整诊断路径均有覆盖。
- [x] NoInterleave 的非法 yield 使用独立 `ErrRuntimeInvariant`。
- [x] README 与 API 注释描述当前 NoInterleave / NoYield 行为。
- [x] CHANGELOG 明确记录 `ErrYieldForbidden` 行为变化。
- [x] 普通 actor 协作调用环保持合法，actor 不依赖 `observe/waitgraph`。

## 五、原 Part B 待办关闭记录

2026-08-24 的 cluster runtime 提交以 call-path 方案关闭跨服务和跨节点环检测、完整
路径诊断及 invariant 错误区分。2026-08-25 又将环身份补全为 `(Node, Service)`，避免
不同节点的同名服务产生误报，并完成单一 context carrier。原文所称“四个红测试”和
“waitgraph / ctx key 暂缓”均不再代表当前实现状态。
