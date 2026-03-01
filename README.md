# data

`data` 是 infrago 的模块包。

## 安装

```bash
go get github.com/infrago/data@latest
```

## 最小接入

```go
package main

import (
    _ "github.com/infrago/data"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[data]
driver = "default"
```

## 公开 API（摘自源码）

- `func ParseQuery(args ...Any) (Query, error)`
- `func Ref(field string) FieldRef { return FieldRef(strings.TrimSpace(field)) }`
- `func (e *DataError) Error() string`
- `func (e *DataError) Unwrap() error`
- `func (e *DataError) Is(target error) bool`
- `func Error(op string, code error, err error) error`
- `func ErrorKind(err error) string`
- `func QuerySignature(q Query) string`
- `func (m *Module) Stats(names ...string) Stats`
- `func CacheToken(name string, tables []string) string`
- `func TouchTableCache(name, table string) uint64`
- `func (m *Module) Tables() map[string]Table`
- `func (m *Module) Views() map[string]View`
- `func (m *Module) Models() map[string]Model`
- `func (m *Module) TableConfig(name string) *Table`
- `func (m *Module) ViewConfig(name string) *View`
- `func (m *Module) ModelConfig(name string) *Model`
- `func (m *Module) Field(name, field string, extends ...Any) Var`
- `func (m *Module) Fields(name string, keys []string, extends ...Vars) Vars`
- `func (m *Module) Options(name, field string) Map`
- `func (m *Module) Option(name, field, key string) Any`
- `func SnakeFieldPath(field string) string`
- `func CamelFieldPath(field string) string`
- `func (v *sqlView) Count(args ...Any) int64`
- `func (v *sqlView) First(args ...Any) Map`
- `func (v *sqlView) Query(args ...Any) []Map`
- `func (v *sqlView) Aggregate(args ...Any) []Map`
- `func (v *sqlView) Scan(next ScanFunc, args ...Any) Res`
- `func (v *sqlView) ScanN(limit int64, next ScanFunc, args ...Any) Res`
- `func (v *sqlView) Slice(offset, limit int64, args ...Any) (int64, []Map)`
- `func (v *sqlView) Group(field string, args ...Any) []Map`
- `func (m *Module) Base(names ...string) DataBase`
- `func (b *sqlBase) Close() error`
- `func (b *sqlBase) WithContext(ctx context.Context) DataBase`
- `func (b *sqlBase) WithTimeout(timeout time.Duration) DataBase`
- `func (b *sqlBase) Error() error`
- `func (b *sqlBase) ClearError()`
- `func (b *sqlBase) Begin() error`
- `func (b *sqlBase) Commit() error`
- `func (b *sqlBase) Rollback() error`

## 排错

- 模块未运行：确认空导入已存在
- driver 无效：确认驱动包已引入
- 配置不生效：检查配置段名是否为 `[data]`
