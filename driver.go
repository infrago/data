package data

import (
	"database/sql"

	. "github.com/infrago/base"
)

type (
	// Driver 数据驱动
	Driver interface {
		Connect(name string, config Config) (Connect, error)
	}

	// Connect 会话连接
	Connect interface {
		Open() error
		Health() (Health, error)
		Close() error

		Base() DataBase
	}

	Health struct {
		Workload int64
	}
	// DataTrigger struct {
	// 	Name  string
	// 	Value Map
	// }

	BatchFunc func() Res

	DataBase interface {
		Close() error
		Erred() error

		Table(name string) DataTable
		View(name string) DataView
		Model(name string) DataModel

		Serial(key string, start, step int64) int64
		Break(key string)

		//开启手动提交事务模式
		Begin() (*sql.Tx, error)
		Submit() error
		Cancel() error
		Batch(BatchFunc) Res
	}

	DataTable interface {
		Create(Map) Map
		Change(Map, Map) Map
		Remove(...Any) Map
		Update(sets Map, args ...Any) int64
		Delete(args ...Any) int64

		Entity(Any) Map
		Count(args ...Any) float64
		First(args ...Any) Map
		Query(args ...Any) []Map
		Limit(offset, limit Any, args ...Any) (int64, []Map)
		Group(field string, args ...Any) []Map
	}

	//数据视图接口
	DataView interface {
		Count(args ...Any) float64
		First(args ...Any) Map
		Query(args ...Any) []Map
		Limit(offset, limit Any, args ...Any) (int64, []Map)
		Group(field string, args ...Any) []Map
	}

	//数据模型接口
	DataModel interface {
		First(args ...Any) Map
		Query(args ...Any) []Map
	}
)
