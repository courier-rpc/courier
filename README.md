# Courier

[![Go Reference](https://pkg.go.dev/badge/github.com/simpossible/courier.svg)](https://pkg.go.dev/github.com/simpossible/courier)
[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.21-blue)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Courier 是一个基于 MQTT 的轻量级 RPC 框架，专为 **客户端与服务端** 之间的通信设计。

## 设计目标

### 为什么不是 gRPC？

gRPC 非常适合服务端之间的调用，但在客户端场景下存在局限：

- **协议较重** — 依赖 HTTP/2，嵌入式设备和浏览器支持差
- **不支持推送** — 纯请求-响应模型，服务端无法主动下发消息
- **需要代理** — 浏览器端需 gRPC-Web 代理，增加架构复杂度

Courier 选择 MQTT 作为传输层：

- **协议轻量** — 几乎所有平台都有 MQTT 客户端库
- **天然双向** — pub/sub 模型原生支持服务端推送
- **移动友好** — 断线重连、低带宽、心跳保活
- **零代码负载均衡** — `$share` 共享订阅，Broker 自动分发

### 典型架构

```
┌──────────┐   MQTT (Courier)   ┌──────────┐
│  Client  │◄──────────────────►│  Server  │
│ (App/IoT)│   QoS 0 + 重试      │  (Go)    │
└──────────┘                     └────┬─────┘
                                      │ gRPC
                                ┌─────┴─────┐
                                │  Service B │
                                └────────────┘
```

- **客户端 ↔ 服务端**：Courier over MQTT
- **服务端 ↔ 服务端**：gRPC

### 核心特性

- **Protobuf + 代码生成** — `protoc-gen-courier` 插件自动生成类型安全的 RPC 代码
- **共享订阅负载均衡** — `$share/{group}/mrpc/request/{service}`，多实例自动分担请求
- **客户端超时 + 重试** — pending map + 可配置退避策略，QoS 0 + 应用层可靠投递
- **断线自动重连** — 自动重订阅所有 topic，包括 `$share` 共享订阅
- **拦截器链** — 日志、认证、指标、panic 恢复，与 gRPC interceptor 用法一致
- **Session 管理** — 服务端 session 替代 JWT，省带宽；支持内存和 Redis 存储
- **不依赖插件** — 手写 `ServiceInfo` 也能注册，灵活选择

## 快速开始

### 安装

```bash
go get github.com/simpossible/courier
```

### 定义 RPC（Protobuf）

```protobuf
syntax = "proto3";
package user;

import "options.proto"; // courier 的 cmd 扩展

message EmailCodeGetReq {
  string email = 1;
}

message EmailCodeGetResp {
  int32 code = 1;
  string msg = 2;
}

service RegisterService {
  rpc EmailGetCode(EmailCodeGetReq) returns (EmailCodeGetResp) {
    option (courier.cmd) = 10001;
  }
}
```

生成代码：

```bash
protoc --go_out=. --courier_out=. user.proto
```

### 服务端

```go
package main

import (
    "github.com/simpossible/courier/rpc"
    "github.com/simpossible/courier/transport"
    "myapp/proto/user"
)

type RegisterHandler struct{}

func (h *RegisterHandler) EmailGetCode(ctx *rpc.Context, req *user.EmailCodeGetReq) (*user.EmailCodeGetResp, error) {
    // 业务逻辑
    return &user.EmailCodeGetResp{Code: 0, Msg: "OK"}, nil
}

func main() {
    tp := transport.NewMQTTTransport(
        transport.WithBrokers("tcp://localhost:1883"),
    )

    srv := rpc.NewServer(
        rpc.WithServerTransport(tp),
        rpc.WithServiceName("RegisterService"),
    )

    srv.Register(user.RegisterRegisterService(&RegisterHandler{}))

    if err := srv.Start(); err != nil {
        panic(err)
    }
    select {} // block forever
}
```

### 客户端

```go
package main

import (
    "context"
    "google.golang.org/protobuf/proto"
    "github.com/simpossible/courier/rpc"
    "github.com/simpossible/courier/transport"
    "myapp/proto/user"
)

func main() {
    tp := transport.NewMQTTTransport(
        transport.WithBrokers("tcp://localhost:1883"),
    )

    client := rpc.NewClient(
        rpc.WithClientTransport(tp),
        rpc.WithClientID("device-abc123"),
        rpc.WithTimeout(10 * time.Second),
        rpc.WithRetry(3, 1*time.Second, 1.5),
    )

    if err := client.Connect(); err != nil {
        panic(err)
    }

    payload, _ := proto.Marshal(&user.EmailCodeGetReq{Email: "test@example.com"})
    respBytes, err := client.Call(context.Background(), "RegisterService", 10001, payload)
    if err != nil {
        panic(err)
    }

    resp := &user.EmailCodeGetResp{}
    proto.Unmarshal(respBytes, resp)
    println(resp.Msg) // "OK"
}
```

### 手写注册（不用 protoc 插件）

```go
srv.Register(courier.ServiceInfo{
    ServiceName: "MyService",
    Methods: []courier.MethodInfo{
        {
            Cmd:  20001,
            Name: "Ping",
            Handle: func(ctx *courier.Context, raw []byte) ([]byte, error) {
                return []byte("pong"), nil
            },
        },
    },
})
```

## 文档

| 文档 | 内容 |
|------|------|
| [架构设计](doc/architecture.md) | 分层架构、协议格式、请求-响应流程 |
| [配置参考](doc/configuration.md) | Server / Client / Transport 全部配置项 |
| [拦截器](doc/interceptors.md) | 内置拦截器 + 自定义拦截器写法 |
| [Session 管理](doc/session.md) | 服务端 Session、内存/Redis 存储、带宽对比 |
| [共享订阅](doc/shared-subscription.md) | `$share` 机制、负载均衡、水平扩展 |
| [Protoc 插件](doc/codegen.md) | 安装、使用、生成代码结构 |

## 设计哲学

- **传输层可替换** — `Transport` 接口抽象，未来可支持 WebSocket、QUIC
- **核心不绑定 Protobuf** — `HandlerFunc` 处理 `[]byte`，序列化在适配层
- **无全局状态** — Server / Client 都是实例，可创建多个
- **QoS 0 + 应用层重试** — 传输层不做消息重放，重试策略由调用方控制
- **协议简洁** — 固定 header + payload，无复杂帧协商

## License

[MIT](LICENSE)

### 精确调用设备

服务端使用 `rpc.WithServerDeviceID("device-001")` 开启设备入口；调用时追加 `rpc.WithTargetDevice("device-001")`，即可直达该设备。不传调用选项继续使用原有共享分发。详见 [共享订阅与设备路由](doc/shared-subscription.md#按设备-id-精确调用)。

### GZIP 压缩

客户端配置 `rpc.WithCompression(codec.CompressionGZIP)`，或调用时追加 `rpc.WithCallCompression(codec.CompressionGZIP)`，即可对请求和响应 payload 启用 GZIP。v2 header 携带算法类型（0=None、1=GZIP），接收端自动解压。默认仍发送 v1，启用前需升级服务端。详见 [压缩协议与用法](doc/compression.md)。

### 本地 MQTT 联调

运行 `./scripts/test-integration.sh`，自动启动临时 MQTT 5 broker 和两个服务实例，依次运行 Go、JS、Dart、Swift 的真实网络测试，并在成功、失败或中断后关闭服务。无需 Docker；依赖已安装可加 `--skip-install`。环境要求及单端入口见 [联调说明](integration/README.md)。
