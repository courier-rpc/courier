# 架构设计

## 分层架构

Courier 分为三层，每层职责单一、可独立替换：

```
┌─────────────────────────────────────────┐
│            Application Code             │
├─────────────────────────────────────────┤
│  rpc/   │  Server, Client, Interceptor  │  ← RPC 核心
├─────────┤───────────────────────────────┤
│ transport/ │  Transport 接口 + MQTT 实现 │  ← 传输层
├───────────┤─────────────────────────────┤
│   codec/  │  Request / Response 编解码   │  ← 协议层
└───────────┴─────────────────────────────┘
```

| 层 | 包 | 职责 |
|---|---|---|
| 协议层 | `codec/` | 二进制帧的编码/解码，零外部依赖 |
| 传输层 | `transport/` | MQTT 连接管理、断线重连、自动重订阅 |
| RPC 层 | `rpc/` | 服务注册、命令路由、请求-响应匹配、拦截器 |

## 二进制协议

以下为默认 v1 格式；带压缩算法字段的 v2 格式见 [GZIP 压缩](compression.md)。

### Request Frame

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Length (4B, BE)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     Version (2B, BE)  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Cmd (4B, BE)                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         RequestID (16B)                       |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   ExtensionsLen (2B, BE)      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Extensions (...B, 可选)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload (...B)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Length** (uint32, BigEndian): 整帧长度 = 28 + len(Extensions) + len(Payload)
- **Version** (uint16, BigEndian): 协议版本，默认 1，启用压缩选项时为 2
- **Cmd** (uint32, BigEndian): 命令号，用于路由到对应的处理函数
- **RequestID** (16B): 来自请求的唯一标识，用于匹配请求和响应
- **ExtensionsLen** (uint16, BigEndian): 扩展段字节数，0 表示无扩展
- **Extensions**: 扩展数据，可用于 token、签名、时间戳等（可用 Protobuf 序列化）
- **Payload**: Protobuf 序列化的请求体

> **ClientID 不在帧中传输。** 服务端通过 Broker（EMQX）注入的消息属性获取发布者的 ClientID，用于 session 查询和响应路由。
> **Extensions 设计类似 HTTP Header**，固定 header（Length ~ ExtensionsLen）用于快速路由分发，Extensions 段延迟到需要鉴权时解析。

### Response Frame

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Length (4B, BE)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
+                       RequestID (16B)                         +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Code (4B, BE)                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Payload (...B)                        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Length** (uint32, BigEndian): 整帧长度 = 24 + len(Payload)
- **RequestID** (16B): 来自请求的唯一标识，用于匹配请求和响应
- **Code** (uint32, BigEndian): 响应状态码，0 表示成功，非 0 表示错误
- **Payload**: 成功时为 Protobuf 序列化的响应体；失败时为错误信息字符串

> **错误处理：** 业务层通过 `rpc.NewError(code, msg)` 返回错误，Code 会被写入响应帧的 Code 字段。如果返回的是普通 `error` 而非 `*rpc.Error`，Code 统一为 2。Client 端在 codec 层通过 Code 判断成功/失败，Code != 0 时直接返回错误。

## 请求-响应流程

```
Client                                 Server
  │                                      │
  │  1. Generate RequestID (UUID)        │
  │  2. Encode Request Frame (cmd + payload)
  │  3. Store in pending map             │
  │  4. Set timeout timer                │
  │                                      │
  │──── Publish to mrpc/request/{svc} ──►│
  │                                      │  5. Decode Request Frame
  │                                      │  6. Extract ClientID from transport properties (EMQX)
  │                                      │  7. Dispatch by Cmd
  │                                      │  8. SessionInterceptor: lookup session by ClientID
  │                                      │  9. Call HandlerFunc
  │                                      │  10. Encode Response Frame
  │                                      │
  │◄── Publish to mrpc/response/{clientID}
  │                                      │
  │  11. Match RequestID in pending      │
  │  12. Stop timer, return response     │
  │                                      │
```

如果 timer 先于响应触发（超时），Client 返回 `ErrTimeout` 并从 pending map 中移除条目。
如果配置了重试，Client 在等待期间按 backoff 间隔重新发布请求。

## 共享订阅

多个 Server 实例通过 `$share` 订阅同一 topic：

```
Server A 订阅: $share/RegisterService/mrpc/request/RegisterService
Server B 讂阅: $share/RegisterService/mrpc/request/RegisterService
Server C 订阅: $share/RegisterService/mrpc/request/RegisterService
```

Broker 收到请求后只投递给其中一个实例（round-robin 或随机），天然的负载均衡。
新实例上线自动加入分发，下线自动移除，无需额外服务注册。

## 断线重连

`transport.MQTTTransport` 内置了完整的重连机制：

1. **断线时** — `ConnectionLostHandler` 仅打日志，不做清理
2. **重连成功时** — `OnConnectHandler` 触发 `resubscribeAll()`：
   - RLock 拷贝订阅表
   - 解锁后逐个重新订阅（包括 `$share` topic）
3. **客户端 pending** — 断线期间的 pending 请求：
   - timer 超时 → 返回 `ErrTimeout`，清理 pending
   - 重连成功 → response topic 已重订阅，响应正常匹配
