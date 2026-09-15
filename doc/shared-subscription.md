# 共享订阅 ($share)

## 原理

MQTT 5.0 规范（以及 EMQX、Mosquitto 等 MQTT 3.1.1 broker 的扩展）支持共享订阅语法：

```
$share/{group_name}/{real_topic}
```

同一个 `group_name` 下的多个订阅者形成一个消费组。Broker 收到消息后，
只投递给其中一个订阅者，而不是广播给所有人。这就是天然的负载均衡。

## Courier 中的使用

### Topic 命名

```
服务端订阅:  $share/{serviceName}/mrpc/request/{serviceName}
客户端发布:  mrpc/request/{serviceName}
```

例如 `RegisterService`：

```
Server A 订阅: $share/RegisterService/mrpc/request/RegisterService
Server B 订阅: $share/RegisterService/mrpc/request/RegisterService
Server C 订阅: $share/RegisterService/mrpc/request/RegisterService

Client 发布:   mrpc/request/RegisterService
```

### 多实例水平扩展

```go
// 节点 1
srv1 := rpc.NewServer(
    rpc.WithServerTransport(tp1),
    rpc.WithServiceName("RegisterService"),
    rpc.WithSharedSubscribe(true),  // 默认就是 true
)
srv1.Register(user.RegisterRegisterService(&handler{}))
srv1.Start()

// 节点 2（完全相同的代码，只是不同的 transport 实例）
srv2 := rpc.NewServer(
    rpc.WithServerTransport(tp2),
    rpc.WithServiceName("RegisterService"),
    rpc.WithSharedSubscribe(true),
)
srv2.Register(user.RegisterRegisterService(&handler{}))
srv2.Start()
```

新节点上线后 Broker 自动将其加入分发，下线后自动移除。无需服务注册、心跳或配置中心。

### 关闭共享订阅

如果你的 broker 不支持 `$share`，可以关闭：

```go
srv := rpc.NewServer(
    rpc.WithSharedSubscribe(false),
    // ...
)
```

此时订阅普通 topic `mrpc/request/{serviceName}`，每个实例都会收到全量消息。

## Broker 兼容性

| Broker | `$share` 支持 | 说明 |
|--------|---------------|------|
| EMQX | 支持 | 原生支持，推荐 |
| Mosquitto | 支持 (≥2.0) | 2.0 及以上版本支持 `$share` |
| HiveMQ | 支持 | 原生支持 |
| VerneMQ | 支持 | 原生支持 |
| ActiveMQ | 不确定 | 需要验证 |

## 断线重订阅

`$share` 订阅是基于连接的。断线重连后必须重新订阅。
Courier 的 `transport.MQTTTransport` 在 `OnConnectHandler` 中自动重新订阅所有 topic，
包括 `$share` 共享订阅，无需手动处理。

## 负载均衡策略

负载均衡策略由 Broker 实现，Courier 不控制。常见策略：

- **round-robin** — 轮询分发
- **random** — 随机选择
- **sticky** — 基于 clientID 哈希（部分 broker 支持）

如果需要粘性路由（同一 client 总是打到同一 server），需要在 broker 侧配置。

## 按设备 ID 精确调用

每个终端都可以提供同名服务，并通过稳定且唯一的设备 ID 接收定向请求：

```go
server := rpc.NewServer(
    rpc.WithServerTransport(deviceTransport),
    rpc.WithServiceName("DeviceService"),
    rpc.WithServerDeviceID("device-001"),
)
// 按原有方式 server.Register(...)，然后 server.Start()。

// 不指定设备：由共享订阅选择一个实例。
resp, err := client.Call(ctx, "DeviceService", cmd, payload)

// 指定设备：仅 device-001 接收请求。
resp, err = client.Call(ctx, "DeviceService", cmd, payload,
    rpc.WithTargetDevice("device-001"))
```

配置设备 ID 后，服务端同时订阅：

- `$share/DeviceService/mrpc/request/DeviceService`：共享分发入口。
- `mrpc/request/DeviceService/device/device-001`：普通订阅，设备专属入口。

定向调用只向设备专属入口发布；重试仍发送到同一设备，设备离线或未提供服务时按原有超时返回，不会回退到其他设备。请求帧、命令 ID、响应 topic 和处理器无需改变。不配置 `WithServerDeviceID` 时保持原有行为；`WithSharedSubscribe(false)` 仍表示普通广播入口，设备专属入口不受影响。

同一服务下设备 ID 必须唯一，否则多个订阅者都会收到该设备的请求。ID 不允许为空（调用目标）、包含 `/`、`+`、`#` 或 NUL。服务端空 ID 表示不启用设备入口。设备 ID 是路由标识，调用方的 `WithClientID` 则用于接收响应，需与其 MQTT ClientID 一致；显式配置的 MQTT ClientID 现在保持原值，多个连接应各自使用唯一 ClientID。

Go 终端可同时使用 Server 和 Client；当前它们各自管理 transport 的连接和关闭，因此应使用独立 transport 和不同 MQTT ClientID。JS / Flutter 此次新增的是定向调用能力，尚未提供服务端注册 API。

响应仍依赖现有 broker 规则注入发布方 `client_id`。部署时需将新增的设备 topic 纳入现有 ACL 和消息属性注入规则。
