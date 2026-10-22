# Pitaya 项目学习指南

Pitaya 是一个用 Go 编写的游戏服务端框架，负责连接管理、消息分发、会话管理和服务器间通信。房间规则、账号系统、战斗逻辑、数据持久化等业务需要在框架之上实现。

这份指南面向具备 Go 基础、第一次阅读本项目的开发者，以当前仓库的 v3 实现为准，代码基线为 `main` 的 `f3d0af5`。建议先跑通一个请求，再顺着请求找到相关模块，最后通过小改动验证理解。第一轮学习结束时，你应该能解释：客户端请求如何到达后端、后端如何找到这个玩家，以及响应如何回到客户端。

## 1. 先建立项目地图

最先区分两个入口：仓库根目录的 `main.go` 启动的是命令行客户端；游戏服务器示例的入口在 `examples/`，框架实现位于 `pkg/`。

| 路径 | 职责 | 学习顺序 |
| --- | --- | --- |
| [examples/demo/cluster_grpc](../examples/demo/cluster_grpc) | connector 与 room 的 gRPC 集群示例 | 首先阅读并运行 |
| [pkg/builder.go](../pkg/builder.go) | 创建并组装框架依赖 | 理解服务器如何构建 |
| [pkg/app.go](../pkg/app.go) | 对外接口、启动、连接接入与关闭 | 理解应用生命周期 |
| [pkg/component](../pkg/component) | 组件、方法发现、注册选项 | 理解业务接口如何注册 |
| [pkg/service](../pkg/service) | 消息分流、Handler 调用、跨服请求 | 核心阅读区域 |
| [pkg/agent](../pkg/agent) 与 [pkg/session](../pkg/session) | 连接代理与玩家会话 | 理解连接和用户的关系 |
| [pkg/route](../pkg/route) 与 [pkg/router](../pkg/router) | 解析路由字符串、选择目标服务器 | 理解请求去向 |
| [pkg/cluster](../pkg/cluster) | etcd 服务发现、NATS 和 gRPC RPC | 第二轮深入 |
| [pkg/conn](../pkg/conn) 与 [pkg/serialize](../pkg/serialize) | 网络协议与业务数据序列化 | 理解字节如何成为参数 |
| [pkg/pipeline](../pkg/pipeline) | Handler 和 Remote 调用钩子 | 实现校验等通用逻辑 |
| [pkg/groups](../pkg/groups) 与 [pkg/modules](../pkg/modules) | 用户组、广播、生命周期扩展 | 阅读业务扩展方式 |
| [pkg/metrics](../pkg/metrics) 与 [pkg/tracing](../pkg/tracing) | 指标与 OpenTelemetry 链路追踪 | 排查延迟和故障 |
| [pkg/worker](../pkg/worker) | Redis 支持的异步 RPC 任务 | 按需学习 |
| [cmd](../cmd) 与 [repl](../repl) | CLI 命令和交互式客户端 | 调试时查看 |
| [e2e](../e2e) 与 [examples/testing](../examples/testing) | 端到端测试和依赖服务 | 修改框架后的验证入口 |
| [xk6-pitaya](../xk6-pitaya) | k6 扩展，独立 Go module | 性能测试阶段阅读 |

先补齐这些 Go 知识即可开始：接口与依赖注入、goroutine 与 channel、`context.Context`、结构体标签、指针与错误返回。反射和 protobuf 可以读到对应模块时再深入。

## 2. 理解服务器角色和核心概念

### 2.1 模式和角色是两件事

`Standalone` 与 `Cluster` 决定是否使用服务发现和跨服 RPC；`Frontend` 与服务器类型决定这个进程承担什么角色。

- `Standalone`：构建时不允许存在 ServiceDiscovery、RPCClient、RPCServer。
- `Cluster`：构建时三者必须齐备。
- 前端服务器：持有客户端 TCP 或 WebSocket 连接，例如 `connector`。
- 后端服务器：处理转发过来的请求和服务器间 RPC，例如 `room`。
- 服务器类型：一类服务的名字，例如 `room`，同一类型可以有多个实例。
- 服务器 ID：具体实例的标识。发往 `room` 的请求还需要选出一个实例。

这些约束可在 `Builder.Build`、`Builder.AddAcceptor` 和 `App.Start` 中找到。示例中 room 虽然接收 `--port 3251` 参数，但后端不会添加客户端 Acceptor；它使用 `--rpcsvport 3435` 接收 gRPC 请求。

### 2.2 把几个对象对应到职责

| 概念 | 负责什么 | 不应混淆的对象 |
| --- | --- | --- |
| Acceptor | 监听端口，接收客户端连接 | 业务 Handler |
| Agent | 代理一条客户端连接，处理发送、心跳等 | 玩家账号 |
| Session | 保存连接 ID、绑定的 UID、会话数据 | 持久化玩家数据库 |
| Component | 一组业务方法及生命周期回调 | 一个独立进程 |
| Handler | 客户端可以请求或通知的方法 | 服务间 Remote 方法 |
| Remote | 服务间显式 RPC 调用的方法 | 所有部署在后端的方法 |
| ServiceDiscovery | 找到某类型服务器的实例 | 消息传输本身 |
| Router | 从候选实例中选一个目标 | 路由字符串解析器 |
| RPCClient / RPCServer | 传输和接收跨进程请求 | 客户端 TCP 协议 |

后端也可以注册 Handler。客户端通过前端转发来调用它，并不意味着它必须注册成 Remote。

## 3. 跑通第一个跨服请求

下面的 shell 命令均在仓库根目录执行。需要 Go、make；使用所给依赖启动命令时还需要 Docker Compose。当前 [go.mod](../go.mod) 和 [go.work](../go.work) 都声明 `go 1.25.4`，应准备满足该要求的工具链，不能沿用旧 README 中的 Go 1.16 要求。

### 3.1 准备依赖

```bash
go version
git submodule update --init --recursive
go mod download
make build
```

`make build` 的产物是 `build/pitaya-cli`。`make setup` 也存在，但当前实现调用 `go get ./...`，可能改变依赖文件；初次阅读时使用上面明确的准备步骤即可。`pitaya-protos` 是子模块，生成文件 `pkg/protos/*.pb.go` 不应手工修改。

### 3.2 启动 etcd 和两个服务器

先启动服务发现依赖：

```bash
docker compose -f examples/testing/docker-compose.yml up -d etcd
```

终端 A 启动 connector：

```bash
make run-cluster-grpc-example-connector
```

终端 B 启动 room：

```bash
make run-cluster-grpc-example-room
```

此示例需要 etcd，不需要 NATS 或 Redis。connector 的客户端 TCP 端口为 `3250`，两个进程的 gRPC 端口分别为 `3434`、`3435`。等两个进程都输出 `all modules started!` 后，再发送请求；若刚启动时尚未发现 room，稍后重试。

### 3.3 使用 CLI 请求

终端 C 启动交互客户端，注意 `repl` 子命令：

```bash
./build/pitaya-cli repl
```

在 REPL 内逐条输入，每条等待返回后再执行下一条：

```text
connect localhost:3250
request room.room.entry
request connector.getsessiondata
request room.room.getsessiondata
```

`room.room.entry` 成功时应返回 `{"code":0,"result":"ok"}`。对应代码是 [Room.Entry](../examples/demo/cluster_grpc/services/room.go)：它获取 Session，用 Session ID 构造演示 UID，调用 `Bind` 后返回结果。这种演示 UID 不代表生产环境的账号认证方案。

`connector.getsessiondata` 是两段路由，访问当前 connector 上的 Handler；`room.room.getsessiondata` 是三段路由，需要转发到 room。返回的 Session 数据字段名为 `Data`。

### 3.4 验证会话同步和广播

在同一连接中继续执行：

```text
request connector.setsessiondata {"Data":{"level":1}}
request room.room.getsessiondata
request room.room.setsessiondata {"Data":{"level":2}}
request connector.getsessiondata
request room.room.join
notify room.room.message {"name":"learner","content":"hello"}
```

你应观察到：room 能读到前端设置的 `level=1`；room 更新并执行 `PushToFront` 后，前端能读到 `level=2`；加入组后可收到 `onMembers`，发送消息后可收到 `onMessage`。后端 `SetSessionData` 返回的是原始字节 `success`，不是 JSON 对象。`SetData` 设置整份数据，不是仅更新传入的某几个键。

`message` 没有返回值，因此使用 `notify`；它没有与 Request 对应的直接响应，广播 Push 是另一条消息。可以打开第二个 CLI，依次执行连接、`entry`、`join`，观察两个客户端收到广播。

学习完成后，分别在服务器终端按 Ctrl+C 结束进程。如这些 Compose 容器仅用于本次练习，可运行：

```bash
docker compose -f examples/testing/docker-compose.yml down
```

该命令会停止这个 Compose 项目的全部服务，包括同项目中可能已经运行的 NATS、Redis。

## 4. 从示例读懂构建和启动

打开 [examples/demo/cluster_grpc/main.go](../examples/demo/cluster_grpc/main.go)，按以下顺序阅读：

1. `main` 读取端口、服务器类型、前后端角色，构造 gRPC 地址元数据。
2. `createApp` 创建 Builder。默认 Cluster 配置使用 NATS RPC，此处用 gRPC Server 和 Client 替换对应字段。
3. 创建 `ETCDBindingStorage`，并传入 gRPC Client，用于按用户定位其前端连接所在服务器。
4. 前端添加 TCP Acceptor；随后 `builder.Build()` 组装 App。
5. `RegisterModule` 注册绑定存储模块，`configureFrontend` 或 `configureBackend` 注册业务方法。
6. `app.Start()` 启动应用并等待停止信号，退出路径执行资源清理。

接着读 [pkg/builder.go](../pkg/builder.go)：`NewBuilder` 选择默认实现，`Build` 把这些实现连起来。重点看 HandlerPool 如何同时传给 HandlerService 和 RemoteService：本地请求与转发到后端的请求，最终可以共用同一套 Handler 执行逻辑。

最后读 [pkg/app.go](../pkg/app.go) 的 `Start` 和 `listen`：它们安排组件初始化、Dispatch goroutine、Acceptor、模块启动和停止处理。Cluster 模式将服务发现放在模块启动序列的后面，使实例对集群可见前，其他模块先完成初始化。

读完后应能回答：为什么替换 RPC 实现要在 `Build` 之前？为什么前端必须添加 Acceptor？为什么 `Build` 完成还不等于服务已启动？

## 5. 沿着一个请求读源码

### 5.1 先理解路由

`room.room.entry` 的含义依次是：服务器类型 `room`、组件名 `room`、方法名 `entry`。两个 `room` 含义不同，只是示例取了相同的名字。

[route.Decode](../pkg/route/route.go) 支持两段和三段格式。两段格式没有服务器类型，`HandlerService.processMessage` 会补成本机类型。示例通过 `component.WithName("room")` 指定组件名，通过 `component.WithNameFunc(strings.ToLower)` 把 `Entry` 转为 `entry`；名称转换来自注册选项，不能假定所有项目都使用小写。

### 5.2 网络数据分为三层

```text
TCP / WebSocket 中的字节
  → Packet：握手、握手确认、心跳、业务数据等包类型
  → Message：Request / Notify / Response / Push、消息 ID、路由、数据
  → 业务数据：JSON、protobuf 或原始 []byte
```

对应阅读 [pkg/conn/codec](../pkg/conn/codec)、[pkg/conn/message](../pkg/conn/message) 和 [pkg/serialize](../pkg/serialize)。本示例使用 TCP 接入和 JSON 业务数据；服务器间使用 gRPC，不表示客户端也必须使用 gRPC 或 protobuf。

### 5.3 本地 Handler 链路

用 `connector.getsessiondata` 跟踪以下调用：

```text
Acceptor 接收到连接
  → App.listen 启动 HandlerService.Handle
  → 创建 Agent 和 Session，启动连接处理
  → Handle 读取字节并解包
  → processPacket 处理握手或业务包
  → processMessage 解路由，放入 chLocalProcess
  → Dispatch → localProcess
  → HandlerPool.ProcessHandlerMessage
  → 查找方法 → 检查消息类型 → 解码参数 → BeforeHandler
  → 调用业务方法 → AfterHandler → 序列化结果
  → Session.ResponseMID → Agent 发送 → 客户端
```

核心文件是 [handler.go](../pkg/service/handler.go) 和 [handler_pool.go](../pkg/service/handler_pool.go)。后者负责真正调用业务方法，适合观察参数、Session、钩子和返回值。`ResponseMID` 通过消息 ID 让客户端将响应对应到请求。

### 5.4 跨服 Handler 链路

用 `room.room.entry` 跟踪：

```text
客户端 → connector 的 HandlerService.processMessage
  → chRemoteProcess → Dispatch
  → RemoteService.remoteProcess → remoteCall
  → Router 选择 room 实例 → RPCClient.Call
  → room 的 RPCServer → RemoteService.Call
  → handleRPCSys → 创建 Remote Agent 和后端 Session
  → HandlerPool.ProcessHandlerMessage → Room.Entry
  → RPC 响应回到 connector
  → connector 的 Session.ResponseMID → 客户端
```

在 [remote.go](../pkg/service/remote.go) 中关注 `remoteProcess`、`remoteCall`、`Call`、`handleRPCSys`。前端把请求连同会话信息送过去，后端为该请求创建 Remote Agent；客户端连接仍在前端。

建议断点顺序：前端 `processMessage` → 前端 `remoteCall` → 后端 `handleRPCSys` → `Room.Entry`。调试两个进程时，分别观察 route、Session ID、UID 和 FrontendID；命中同名框架函数时先确认当前进程的服务器角色。

## 6. 区分 Handler 和 Remote

[component/method.go](../pkg/component/method.go) 通过反射筛选可注册的方法。常见签名如下：

```go
// Request Handler：业务参数可以省略，也可以是指针或 []byte。
func (r *Room) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error)

// Notify Handler：没有返回值。
func (r *Room) Update(ctx context.Context, req *UpdateRequest)

// Remote：可省略请求参数；有参数时须实现 proto.Message。
// 响应必须是实现 proto.Message 的指针，第二个返回值为 error。
func (r *Room) Fetch(ctx context.Context, req *protos.RPCMsg) (*protos.RPCRes, error)
```

这些是签名说明，`QueryRequest` 等业务类型需要由应用定义。方法必须导出，接收者之后的第一个参数必须符合 `context.Context`。Request Handler 的第一个返回值应为指针或 `[]byte`，不能随意换成 `string` 或普通值结构体。

- `app.Register` 注册客户端 Handler；它可以运行在前端或后端。
- `app.RegisterRemote` 注册服务间 Remote；通常通过 `app.RPC` 或 `app.RPCTo` 调用。
- 转发客户端请求使用 `RPCType_Sys`，最终走 HandlerPool。
- 显式业务 RPC 使用 `RPCType_User`，最终走 `handleRPCUser`。

示例中的 `Room.SendRPC` 调用 `connector.connectorremote.remotefunc`，可与 `Room.Entry` 对照阅读。即使同一个组件同时被两种方式注册，两个注册过程也会分别筛选符合各自签名的方法。

## 7. Session 为什么需要同步

Session ID 标识连接会话；UID 是业务绑定的用户标识。`Bind` 建立这种关联，方便后续按用户推送、踢下线或查找连接。

前端 Session 关联真实连接。后端处理转发请求时，从请求携带的信息构造 Session，因此后端修改不会自动变成前端状态。阅读 [session.go](../pkg/session/session.go) 的 `Bind`、`Set`、`SetData`、`PushToFront`，再对照 [agent_remote.go](../pkg/agent/agent_remote.go)。

后端修改某个会话字段时，典型写法是：

```go
s := r.app.GetSessionFromCtx(ctx)
if err := s.Set("level", 2); err != nil {
    return nil, err
}
if err := s.PushToFront(ctx); err != nil {
    return nil, err
}
```

这段代码放在返回两个值的后端 Handler 内。`PushToFront` 用来更新前端 Session；向客户端发业务消息使用 `Push`，二者不是同一个操作。前端 Session 不允许再调用 `PushToFront`。

可以暂时去掉示例后端的 `PushToFront`，重复第 3 节的数据操作：后端写入返回成功，前端却仍保留原值。恢复同步调用后，前端才看到更新。这个实验比只记住 API 名字更有帮助。

会话数据也不是多个后端共享的数据库。并发更新同一份数据时，需要业务层考虑更新顺序和覆盖问题；长期状态应由适合的存储与一致性方案管理。

## 8. 服务发现和路由如何配合

[etcd_service_discovery.go](../pkg/cluster/etcd_service_discovery.go) 负责服务器注册、租约维护、发现变化和本地缓存；[router.go](../pkg/router/router.go) 根据服务器类型获取候选实例并选出目标。

默认策略随机选实例。gRPC 示例为 `room` 注册了自定义路由，返回遍历 map 时遇到的第一个实例。Go map 遍历顺序不稳定，因此这个示例不能保证同一房间的请求始终进入同一台 room 服务器。

还要注意：当前 `Router.Route` 对 `RPCType_User` 直接使用默认选择逻辑，随后才处理其他类型的自定义路由。不要认为 `AddRoute` 自动控制所有 `app.RPC` 调用。需要指定实例的显式 RPC，可继续阅读 [pkg/rpc.go](../pkg/rpc.go) 的 `RPCTo`。

多房间业务通常需要根据房间 ID 找到承载它的实例，并处理实例退出后的恢复。Group 只表达一组用户及广播能力，不能直接代替房间状态管理；示例的 `MemoryGroupService` 也只保存当前进程的组数据。

## 9. 并发和配置是第二轮重点

### 9.1 不要假定业务方法串行执行

`App.listen` 根据 `Concurrency.Handler.Dispatch` 创建多个 Dispatch goroutine；当前默认值为 `25`。这些 goroutine 消费本地、远程处理队列，同一组件可能被并发调用。

`Dispatch` 内同步执行 `localProcess` 或 `remoteProcess`。一次缓慢的远程调用会占住当前 Dispatch worker；当多个 worker 都忙时，排队时间会增加。增加 channel 容量只是允许积压更多消息，并不直接提高业务处理速度。

对房间成员列表、计数器、地图状态等共享数据，应检查访问是否需要锁、每房间队列或其他串行化方案。当前调度方式不能作为“同一玩家请求一定串行完成”的保证。修改共享状态后，可对对应包运行 `go test -race`。

### 9.2 先确认应用怎样加载配置

配置有两条常见入口：

- `config.NewDefaultPitayaConfig()`：获得默认结构体，再直接修改字段。
- `config.NewConfig()` 配合 `config.NewPitayaConfig(...)`，或 `NewBuilderWithConfigs(...)`：通过 Viper 读取配置。

`cluster_grpc` 示例使用第一种方式。直接设置 `PITAYA_*` 环境变量，不代表这个示例中的默认配置结构体会自动读取它们。需要使用 Viper 配置入口，或者在代码里修改结构体。

走 Viper 入口时，键中的点转换为下划线，例如 `pitaya.metrics.prometheus.port` 对应 `PITAYA_METRICS_PROMETHEUS_PORT`。对照阅读 [config.go](../pkg/config/config.go)、[viper_config.go](../pkg/config/viper_config.go) 和 [配置参考](configuration.rst)。新增字段时同时检查结构体默认值和 defaults map。

跨服传递业务上下文时，阅读 [pkg/context/context.go](../pkg/context/context.go)，使用 `pitaya.AddToPropagateCtx`、`pitaya.GetFromPropagateCtx` 等入口；普通 `context.WithValue` 的值不会因此自动跨进程传输。不要将连接对象等任意运行时对象放入需要序列化传播的数据中。

## 10. 通过小改动巩固理解

### 练习一：增加一个 Echo Handler

在 `examples/demo/cluster_grpc/services/room.go` 中新增以下类型和方法。这个文件已有 `context` 导入，Room 也已完成 Handler 注册：

```go
type EchoRequest struct {
    Text string `json:"text"`
}

type EchoResponse struct {
    Text string `json:"text"`
}

func (r *Room) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
    return &EchoResponse{Text: req.Text}, nil
}
```

重启 room 后，在已有 CLI 中执行：

```text
request room.room.echo {"text":"hello"}
```

预期返回 `{"text":"hello"}`。然后思考：为什么不用手工修改路由表？为什么没有 protobuf 类型也能经 gRPC 转发？为什么这个方法只满足 Handler 的注册规则？

### 练习二：区分输入错误和业务错误

给 Echo 增加空文本校验，返回有业务错误码的 `pitaya.Error`，参考现有 `Room.Entry` 的写法。分别发送合法 JSON、错误 JSON、空文本，观察错误在哪一步产生：参数解码、业务方法，还是序列化响应。

### 练习三：追踪一次跨服会话修改

在前后端记录 Session ID、UID 和 `level`，完成第 7 节的同步实验。能解释每份数据在哪里产生、何时同步，即达到目标。

### 练习四：启动第二台 room

保留原 room，再在根目录另开终端执行：

```bash
go run examples/demo/cluster_grpc/main.go --type room --frontend=false --rpcsvport 3436
```

重复请求，在日志中观察目标实例。再设计根据房间 ID 选实例的方式。验收重点是明确默认路由、示例路由和业务粘性路由的区别，以及内存 Group 为什么会受到请求分布影响。

## 11. 用测试和指标定位问题

### 11.1 分层验证

先运行与理解内容直接相关、无需外部服务的测试：

```bash
go test ./pkg/route ./pkg/component ./pkg/conn/codec ./pkg/conn/message
go test ./pkg/service ./pkg/session
```

修改并发相关逻辑后，对所改包补充 race 检查，例如：

```bash
go test -race ./pkg/service ./pkg/session
```

全量测试入口可查看 [Makefile](../Makefile)：

- `make unit-test-coverage`：单元测试及覆盖率；部分测试会启动嵌入式依赖。该目标先调用 `kill-testing-deps`，实际会尝试关闭测试 Compose 服务，因此不要与正在使用同一 Compose 项目的练习混跑。
- `make e2e-test-nats`：NATS 传输的端到端测试。
- `make e2e-test-grpc`：gRPC 传输的端到端测试；当前 Makefile 同样通过 `ensure-testing-deps` 启动 Compose 中的依赖。
- `make test`：单元测试及两套端到端测试，会管理测试依赖的启停。

阅读测试时，先看输入与断言，再回头看实现。推荐顺序为 [route_test.go](../pkg/route/route_test.go)、[method_test.go](../pkg/component/method_test.go)、[handler_pool_test.go](../pkg/service/handler_pool_test.go)、[session_test.go](../pkg/session/session_test.go)、[remote_test.go](../pkg/service/remote_test.go)。

### 11.2 常见问题的排查顺序

| 现象 | 优先检查 |
| --- | --- |
| 运行 CLI 后只出现帮助信息 | 是否执行了 `./build/pitaya-cli repl` |
| 连接被拒绝 | connector 是否启动，客户端是否连接 `3250` |
| 找不到 room 实例 | etcd 是否可用，room 是否完成注册，服务器类型是否正确 |
| 找到实例但 gRPC 请求失败 | 元数据中的地址和 RPC 端口是否可达；多机部署不能照抄 `127.0.0.1` |
| Handler not found | 路由的组件名、方法名、大小写及方法签名是否匹配 |
| Request 或 Notify 类型不匹配 | 方法是否返回两个值，客户端命令是否选对 |
| 后端修改会话后前端读不到 | 是否调用并检查 `PushToFront` 的错误 |
| 加入组后收不到广播 | 是否先绑定 UID，前端类型是否正确，加入和广播是否使用同一组存储 |
| 没有监控指标 | 是否启用 Reporter，应用是否读取了所设置的配置 |
| 高负载时请求延迟增大 | 队列积压、Dispatch 占用、RPC 延迟、慢业务与慢客户端 |

Prometheus 和 statsd 默认均未启用。看性能指标前，先核对单位和 Reporter 实现：当前 statsd `ReportHistogram` 返回 `ErrNotImplemented`；`ReportSummary` 使用 DogStatsD timer；默认 `response_time_ns` 的 Prometheus histogram 桶上界只有 `10000 ns`，即 `10 µs`。这些差异会影响图表含义，不能只凭指标名判断。

链路追踪实现位于 `pkg/tracing`，当前 v3 使用 OpenTelemetry。配置与示例见 [tracing.md](tracing.md) 和 Makefile 中的 tracing 目标。阅读旧资料时留意版本：v2 的目录布局和追踪实现与这里不同。

## 12. 建议的学习安排

| 阶段 | 阅读与操作 | 完成标志 |
| --- | --- | --- |
| 第一次，约 1 小时 | 第 1 至 3 节，运行 gRPC 示例 | 请求成功，能说明三个终端各自做什么 |
| 第二次，约 1 至 2 小时 | Builder、App、Handler 链路 | 能画出本地与跨服请求路径 |
| 第三次，约 1 至 2 小时 | 组件注册、Session、Echo 练习 | 新增接口可调用，解释会话同步 |
| 第四次，约 2 小时 | Router、etcd、RPC、第二个 room | 能区分服务类型、实例和路由策略 |
| 第五次，按需深入 | 并发、测试、指标、追踪 | 能定位一次请求的耗时和失败阶段 |

每读完一个模块，记录四件事：输入是什么、输出是什么、状态归谁持有、错误返回到哪里。完成第一轮后，再按自己的目标选择深入方向：业务开发重点读 Session、Group、Pipeline；框架维护重点读协议、生命周期、RPC；性能优化重点读调度、队列、指标和压测扩展。
