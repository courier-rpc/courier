# 本地真实 MQTT 联调

此目录是独立的 Go module，只为测试引入 Mochi MQTT 5 broker。Courier 的生产依赖不变。

## 一条命令运行四端

将仓库放在同一父目录：

```text
workspace/
  courier/
  courier-js/
  courier-flutter/
  courier-swift/
```

需要 Python 3、Go 1.24+、Node/npm、Dart SDK（也可使用 Flutter 自带的 Dart）和 Swift 6.3 工具链。

```bash
cd courier
./scripts/test-integration.sh
```

脚本默认安装 JS/Dart 锁文件或声明中的依赖；Go 根据本目录的 `go.mod` / `go.sum` 构建临时测试程序；Swift 在 Broker 启动前解析依赖并编译测试。依赖已安装时：

```bash
./scripts/test-integration.sh --skip-install
```

Dart 未加入 PATH 时可指定：

```bash
DART_BIN=/path/to/flutter/bin/dart ./scripts/test-integration.sh
```

也支持 `GO_BIN`、`NPM_BIN`、`SWIFT_BIN`，以及非并排检出时的 `COURIER_JS_DIR`、`COURIER_FLUTTER_DIR`、`COURIER_SWIFT_DIR`。

## 单独运行一个 SDK

```bash
# 在 courier 中
./scripts/test-integration.sh --suite go

# 在 courier-js 中
npm run test:integration

# 在 courier-flutter 或 courier-swift 中
./scripts/test-integration.sh
```

JS/Dart/Swift 的入口复用 Go 仓库中的协调脚本；Go 仓库不在 `../courier` 时设置 `COURIER_GO_DIR`。每个入口都会创建自己的临时 broker，不能只检出 JS/Dart/Swift 仓库而没有 Go 测试服务。

## 实际测试内容

每次运行都启动：

1. 一个只监听 `127.0.0.1`、使用系统分配空闲 TCP 端口的 MQTT 5 broker，无持久化。
2. 两个真实 Go `rpc.Server`，提供同名服务，设备 ID 分别为 `device-a`、`device-b`，同时订阅共享入口和设备入口。
3. 所选 SDK 的真实网络客户端；测试代码分别位于各自仓库中。

覆盖 v1、不压缩 v2、GZIP v2、中文大消息、空消息、并发混合版本调用、指定设备、共享订阅单次投递、uint32 错误码与离线设备不回退。测试读取实际传输的 MQTT 帧，验证算法字段、请求 ID 和压缩后的长度。通过按调用方统计的处理次数检查重复投递/错误路由。

测试 broker 的 hook 会移除调用方自带的 `client_id` 属性，再使用真实 MQTT 连接 ClientID 注入它，模拟 Courier 部署所需的 broker 属性注入规则。它不是 EMQX 配置测试，也不测试生产环境的 TLS/认证/ACL。

fixture 会对两台设备分别执行真实的 GZIP RPC 往返，成功后才写入 readiness 文件。脚本向用例注入 `COURIER_MQTT_URL`、`COURIER_INTEGRATION_SERVICE`；普通单元测试没有这些变量时跳过网络用例。

## 清理与故障

成功、用例失败、启动超时及 Ctrl-C/SIGTERM 都会进入清理：停止测试进程、终止 fixture、必要时强制结束，检查监听端口已关闭，并删除临时目录。失败时输出 fixture 日志并以非零状态退出。没有 Docker 容器或后台系统服务。

可以故意让 JS 测试命令失败来核验退出清理（该命令预期退出码为 1）：

```bash
NPM_BIN=/usr/bin/false ./scripts/test-integration.sh --suite js --skip-install
```

Dart 原有的外部 ChatService 测试默认跳过；只有显式设置 `COURIER_CHAT_BROKER_URL` 才会连接指定的外部服务，本脚本不运行它。

### Swift

`../courier-swift/scripts/test-integration.sh` 单独运行 Swift 联调；总入口的 `--suite all` 也包含 Swift。可用 `COURIER_SWIFT_DIR` / `SWIFT_BIN` 指定仓库与工具链。Swift 首次编译下载依赖可能较慢，编译在 Broker 启动前完成。Swift 用例覆盖调用 Go 服务及 Go 回调 Swift 设备服务。
