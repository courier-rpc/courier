# GZIP 消息压缩（可选协议 v2）

请求和响应的固定 header 增加 1 字节 `Compression` 字段：

| 值 | 算法 |
| --- | --- |
| `0` | None，payload 原样传输 |
| `1` | GZIP |
| 其他 | 保留；当前实现拒绝 |

只有 payload 被压缩。header、命令号、请求 ID、响应码和 extensions 保持原始格式。应用处理器与调用方收到的都是解压后的 payload，Protobuf 定义和 MQTT topic 无需改变。压缩适用于共享订阅和按设备 ID 定向调用。

## 使用方法

```go
// 为该客户端启用 GZIP（需先升级服务端）。
client := rpc.NewClient(
    rpc.WithClientTransport(tp),
    rpc.WithClientID("backend"),
    rpc.WithCompression(codec.CompressionGZIP),
)

// 也可仅为一次调用开启 GZIP，并指定设备。
resp, err := client.Call(ctx, "DeviceService", cmd, payload,
    rpc.WithTargetDevice("device-001"),
    rpc.WithCallCompression(codec.CompressionGZIP))

// 覆盖客户端默认压缩选项，使用不压缩的 v2 消息。
resp, err = client.Call(ctx, "DeviceService", cmd, payload,
    rpc.WithCallCompression(codec.CompressionNone))
```

JS 使用 `compression: Compression.GZIP`，Dart 使用 `compression: Compression.gzip`；两端都支持客户端配置和单次调用覆盖。显式指定 None 使用 v2；完全不配置压缩时保持默认 v1。

服务端自动识别请求版本并解压，再调用原有处理器。v2 响应使用请求指定的算法，成功 payload 和错误信息都适用。若处理器返回的内容超过限制，服务端返回一个不压缩的 v2 错误响应。

## 线格式

所有多字节整数为 Big Endian。`Length` 是实际传输的整帧字节数，包括 header、extensions 和压缩后的 payload。

### v2 请求（29 字节固定头）

```text
[Length:4][Version:2 = 2][Cmd:4][RequestID:16][ExtensionsLen:2][Compression:1][Extensions:N][Payload:N]
```

`Compression` 的偏移为 28，extensions 从偏移 29 开始。extensions 不压缩，最大长度为 65535 字节。

### v2 响应（25 字节固定头）

```text
[Length:4][RequestID:16][Code:4][Compression:1][Payload:N]
```

`Compression` 的偏移为 24，payload 从偏移 25 开始。**响应没有独立的 Version 字段，使用对应请求的版本。** RPC 客户端通过偏移 4 的 RequestID 找到 pending call，再选择 v1 或 v2 解码，不根据 payload 猜测版本。同一连接可以并发处理 v1 和 v2 调用。

直接使用 codec 时：

- Go：`EncodeRequestWithCompression`、`EncodeResponseWithCompression`；请求解码仍用 `DecodeRequest`，响应解码用 `DecodeResponseWithVersion(data, codec.CompressionProtocolVersion)`。
- JS：`encodeRequest` / `encodeResponse` 的末尾传入 `Compression.GZIP`；`decodeResponse(data, 2)`。
- Dart：编码函数传入命名参数 `compression: Compression.gzip`；`decodeResponse(data, version: 2)`。

旧编码函数的默认行为仍为 v1：请求头 28 字节、响应头 24 字节，无 Compression 字段。

## 限制与升级

- v2 的原始 payload 与解压后 payload 上限均为 **16 MiB**，解压期间逐步检查，避免大量内存分配。v1 不新增该限制。
- 检查 GZIP 校验和及截断数据；未知版本、未知算法、非法帧长度和 extensions 越界均报错。RPC 收到无法解码的帧时沿用现有行为，丢弃该帧，等待有效响应或调用超时。
- 启用 GZIP 后总是压缩，包括空消息；小消息可能变大。可按调用使用 None，不会自动更换算法。
- 重试复用同一已编码帧。
- **先升级服务端，再启用客户端压缩。** 共享订阅组内所有可能接收 v2 请求的实例都必须升级。新服务端接受 v1；旧服务端不能处理 v2，没有自动协商或降级。
- JS 的响应码现已统一为 Go/Dart 使用的 **uint32**。早期使用 2 字节响应码的端点需要升级，不进行含糊的自动探测。

## 验证

三端各自编码的 v1、v2 None、v2 GZIP 和空 payload 请求/响应保存在各仓库的 `compression.json` 测试夹具中，由三个语言的解码器交叉验证。测试还覆盖 CRC 损坏、截断、非法字段、解压大小限制和设备定向调用。
