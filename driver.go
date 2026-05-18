package data

import (
	"database/sql"

	. "github.com/infrago/base"
)

type (
	Driver interface {
		Connect(*Instance) (Connection, error)
	}

	Connection interface {
		Open() error
		Close() error
		Health() Health
		DB() *sql.DB
		Dialect() Dialect
	}

	Health struct {
		Workload int64
	}

	PoolStats struct {
		Name         string  `json:"name"`
		Driver       string  `json:"driver"`
		Open         int     `json:"open"`
		InUse        int     `json:"inUse"`
		Idle         int     `json:"idle"`
		WaitCount    int64   `json:"waitCount"`
		WaitDuration int64   `json:"waitDurationMs"`
		MaxOpen      int     `json:"maxOpen"`
		Queries      int64   `json:"queries"`
		Writes       int64   `json:"writes"`
		Errors       int64   `json:"errors"`
		CacheHit     int64   `json:"cacheHit"`
		CacheRate    float64 `json:"cacheRate"`
		Slow         int64   `json:"slow"`
		SlowAvgMs    int64   `json:"slowAvgMs"`
		SlowP50Ms    int64   `json:"slowP50Ms"`
		SlowP95Ms    int64   `json:"slowP95Ms"`
	}

	Capabilities struct {
		Dialect       string `json:"dialect"`
		ILike         bool   `json:"ilike"`
		Returning     bool   `json:"returning"`
		Join          bool   `json:"join"`
		Group         bool   `json:"group"`
		Having        bool   `json:"having"`
		Aggregate     bool   `json:"aggregate"`
		KeysetAfter   bool   `json:"keyset_after"`
		JsonContains  bool   `json:"json_contains"`
		ArrayOverlap  bool   `json:"array_overlap"`
		JsonElemMatch bool   `json:"json_elem_match"`
	}

	Dialect interface {
		Name() string
		Quote(string) string
		Placeholder(int) string
		SupportsILike() bool
		SupportsReturning() bool
	}

	ValueBinder interface {
		BindValue(Var, any) (any, bool)
	}

	ValueDecoder interface {
		DecodeValue(Var, any) (any, bool)
	}

	ArrayBinder interface {
		BindArray(any) any
	}

	ParameterLimiter interface {
		MaxParams() int
	}

	ErrorClassifier interface {
		ClassifyError(error) error
	}
)
