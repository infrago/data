package data

import (
	"sync"
	"sync/atomic"

	. "github.com/infrago/base"
)

const queryCompileCacheCapacity = 2048

type queryCompileEntry struct {
	q Query
}

var (
	queryCompileCacheMu    sync.RWMutex
	queryCompileCacheMap   = make(map[string]queryCompileEntry, queryCompileCacheCapacity)
	queryCompileCacheOrder = make([]string, 0, queryCompileCacheCapacity)
	queryCompileEvictIdx   uint64
)

func queryCompileKey(args []Any) string {
	if len(args) == 0 {
		return ""
	}
	return stableAnySignature(args)
}

func loadQueryCompile(key string) (Query, bool) {
	queryCompileCacheMu.RLock()
	entry, ok := queryCompileCacheMap[key]
	queryCompileCacheMu.RUnlock()
	if !ok {
		return Query{}, false
	}
	return cloneQuery(entry.q), true
}

func storeQueryCompile(key string, q Query) {
	queryCompileCacheMu.Lock()
	defer queryCompileCacheMu.Unlock()
	if _, ok := queryCompileCacheMap[key]; ok {
		queryCompileCacheMap[key] = queryCompileEntry{q: cloneQuery(q)}
		return
	}
	if len(queryCompileCacheMap) >= queryCompileCacheCapacity && len(queryCompileCacheOrder) > 0 {
		i := int(atomic.AddUint64(&queryCompileEvictIdx, 1) % uint64(len(queryCompileCacheOrder)))
		old := queryCompileCacheOrder[i]
		delete(queryCompileCacheMap, old)
		queryCompileCacheOrder[i] = key
		queryCompileCacheMap[key] = queryCompileEntry{q: cloneQuery(q)}
		return
	}
	queryCompileCacheOrder = append(queryCompileCacheOrder, key)
	queryCompileCacheMap[key] = queryCompileEntry{q: cloneQuery(q)}
}

func cloneQuery(q Query) Query {
	out := q
	out.Select = cloneStrings(q.Select)
	out.Sort = cloneSorts(q.Sort)
	out.After = cloneMapAny(q.After)
	out.Group = cloneStrings(q.Group)
	out.Aggs = cloneAggs(q.Aggs)
	out.Joins = cloneJoins(q.Joins)
	out.RawParams = cloneAnys(q.RawParams)
	out.Filter = cloneExpr(q.Filter)
	out.Having = cloneExpr(q.Having)
	return out
}

func cloneExpr(e Expr) Expr {
	switch x := e.(type) {
	case nil:
		return nil
	case TrueExpr:
		return x
	case RawExpr:
		return RawExpr{SQL: x.SQL, Params: cloneAnys(x.Params)}
	case CmpExpr:
		return CmpExpr{Field: x.Field, Op: x.Op, Value: x.Value}
	case ExistsExpr:
		return x
	case NullExpr:
		return x
	case NotExpr:
		return NotExpr{Item: cloneExpr(x.Item)}
	case AndExpr:
		items := make([]Expr, 0, len(x.Items))
		for _, item := range x.Items {
			items = append(items, cloneExpr(item))
		}
		return AndExpr{Items: items}
	case OrExpr:
		items := make([]Expr, 0, len(x.Items))
		for _, item := range x.Items {
			items = append(items, cloneExpr(item))
		}
		return OrExpr{Items: items}
	default:
		return x
	}
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneSorts(in []Sort) []Sort {
	if len(in) == 0 {
		return nil
	}
	out := make([]Sort, len(in))
	copy(out, in)
	return out
}

func cloneAggs(in []Agg) []Agg {
	if len(in) == 0 {
		return nil
	}
	out := make([]Agg, len(in))
	copy(out, in)
	return out
}

func cloneJoins(in []Join) []Join {
	if len(in) == 0 {
		return nil
	}
	out := make([]Join, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].On = cloneExpr(in[i].On)
	}
	return out
}

func cloneAnys(in []Any) []Any {
	if len(in) == 0 {
		return nil
	}
	out := make([]Any, len(in))
	copy(out, in)
	return out
}

func cloneMapAny(in Map) Map {
	if len(in) == 0 {
		return nil
	}
	out := Map{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
