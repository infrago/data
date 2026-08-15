# data

`data` 是 infrago 的**模块**。

## 包定位

- 类型：模块
- 作用：统一数据访问模块，负责 SQL/Mongo 风格查询抽象。

## 主要功能

- 对上提供统一模块接口
- 对下通过驱动接口接入具体后端
- 支持按配置切换驱动实现
- 支持数据库内自增序列 `Sequence`

## 序列编号

`DataBase` 现在支持：

```go
db := data.Base("main")
defer db.Close()

id, err := db.Sequence("order.no", 1000, 1)
ids, err := db.SequenceMany("order.no", 3, 1000, 1)
```

语义：

- 第一次调用某个 `key`：返回 `offset`
- 之后每次：返回 `current + step`
- `offset` 只在序列首次创建时生效
- `step == 0` 时按 `1` 处理
- `SequenceMany` 一次原子领取一段编号，例如 `[1000, 1001, 1002]`

实现方式：

- SQL 驱动使用内部表 `_infrago_sequences`
- MongoDB 驱动使用内部集合 `_infrago_sequences`

## 快速接入

```go
import _ "github.com/infrago/data"
```

```toml
[data]
driver = "default"
```

生产连接串可以只从环境变量读取，避免把凭据写入配置文件：

```toml
[data.main]
driver = "postgresql"
url_env = "APP_DATABASE_DSN"
```

`url_env`（也兼容 `dsn_env`）优先于 `url`。变量名只能包含大写字母、数字和下划线且不能以数字开头；变量缺失或为空时启动会立即失败，错误信息不会包含连接串。

## 驱动实现接口列表

以下接口由驱动实现（来自模块 `driver.go`）：

### Driver

- `Connect(*Instance) (Connection, error)`

### Connection

- `Open() error`
- `Close() error`
- `Health() Health`
- `DB() *sql.DB`
- `Dialect() Dialect`

### Dialect

- `Name() string`
- `Quote(string) string`
- `Placeholder(int) string`
- `SupportsILike() bool`
- `SupportsReturning() bool`

## 全局配置项（所有配置键）

配置段：`[data]`

- `driver`：已注册的数据驱动名
- `url`：直接配置的连接串，适合本地开发
- `url_env` / `dsn_env`：保存连接串的环境变量名，适合生产和 CI
- `schema`：默认数据库 schema
- `mapping`：启用字段名映射
- `read_only`：只读连接
- `trash`：软删除配置
- `pool`：连接池配置
- `migrate`：迁移启动与安全策略
- `setting`：驱动专属设置

## 说明

- `setting` 一般用于向具体驱动透传专用参数
- 多实例配置请参考模块源码中的 Config/configure 处理逻辑
