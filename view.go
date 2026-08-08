package data

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type sqlView struct {
	base   *sqlBase
	name   string
	schema string
	source string
	key    string
	fields Vars
}

type sqlPlan struct {
	SQL    string
	Params []Any
}

type cachedSQLPlan struct {
	Plan   sqlPlan
	Hits   atomic.Int64
	UsedAt atomic.Int64
}

var (
	sqlPlanCache      sync.Map
	sqlPlanCacheCount atomic.Int64
)

func (v *sqlView) mapQueryToStorage(q Query) Query {
	if v == nil || v.base == nil || !v.base.fieldMappingEnabled() {
		return q
	}
	if len(q.Select) > 0 {
		out := make([]string, 0, len(q.Select))
		for _, one := range q.Select {
			out = append(out, v.base.storageField(one))
		}
		q.Select = out
	}
	if len(q.Sort) > 0 {
		out := make([]Sort, 0, len(q.Sort))
		for _, one := range q.Sort {
			out = append(out, Sort{Field: v.base.storageField(one.Field), Desc: one.Desc})
		}
		q.Sort = out
	}
	if len(q.Group) > 0 {
		out := make([]string, 0, len(q.Group))
		for _, one := range q.Group {
			out = append(out, v.base.storageField(one))
		}
		q.Group = out
	}
	if len(q.Aggs) > 0 {
		out := make([]Agg, 0, len(q.Aggs))
		for _, one := range q.Aggs {
			field := one.Field
			if field != "*" && field != "" {
				field = v.base.storageField(field)
			}
			out = append(out, Agg{Alias: one.Alias, Op: one.Op, Field: field})
		}
		q.Aggs = out
	}
	if len(q.After) > 0 {
		after := Map{}
		for k, val := range q.After {
			after[v.base.storageField(k)] = val
		}
		q.After = after
	}
	if len(q.Joins) > 0 {
		out := make([]Join, 0, len(q.Joins))
		for _, j := range q.Joins {
			out = append(out, Join{
				From:         j.From,
				Alias:        j.Alias,
				Type:         j.Type,
				LocalField:   v.base.storageField(j.LocalField),
				ForeignField: v.base.storageField(j.ForeignField),
				On:           v.mapExprToStorage(j.On),
			})
		}
		q.Joins = out
	}
	q.Filter = v.mapExprToStorage(q.Filter)
	q.Having = v.mapExprToStorage(q.Having)
	return q
}

func (v *sqlView) mapExprToStorage(expr Expr) Expr {
	switch e := expr.(type) {
	case nil:
		return nil
	case TrueExpr:
		return e
	case AndExpr:
		items := make([]Expr, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, v.mapExprToStorage(one))
		}
		return AndExpr{Items: items}
	case OrExpr:
		items := make([]Expr, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, v.mapExprToStorage(one))
		}
		return OrExpr{Items: items}
	case NotExpr:
		return NotExpr{Item: v.mapExprToStorage(e.Item)}
	case ExistsExpr:
		return ExistsExpr{Field: v.base.storageField(e.Field), Yes: e.Yes}
	case NullExpr:
		return NullExpr{Field: v.base.storageField(e.Field), Yes: e.Yes}
	case RawExpr:
		return e
	case CmpExpr:
		return CmpExpr{
			Field: v.base.storageField(e.Field),
			Op:    e.Op,
			Value: v.mapValueToStorage(e.Value),
		}
	default:
		return e
	}
}

func (v *sqlView) mapValueToStorage(val Any) Any {
	switch vv := val.(type) {
	case FieldRef:
		return Ref(v.base.storageField(string(vv)))
	default:
		return val
	}
}

func (v *sqlView) Count(args ...Any) int64 {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".count.parse", ErrInvalidQuery, err))
		return 0
	}
	q = v.mapQueryToStorage(q)
	v.applyTrashScope(&q)
	if total, ok := v.loadCountCache(q); ok {
		statsFor(v.base.inst.Name).CacheHit.Add(1)
		v.base.setError(nil)
		return total
	}
	if v.base.cacheEnabled() {
		key := v.countCacheKey(q)
		if key != "" {
			statsFor(v.base.inst.Name).CacheMiss.Add(1)
			val, shared, err := cacheSingleflight(v.base.inst.Name, key, func() (cacheValue, error) {
				if total, ok := v.loadCountCache(q); ok {
					return cacheValue{total: total}, nil
				}
				total, err := v.countWithQuery(q)
				if err != nil {
					return cacheValue{}, err
				}
				v.storeCountCache(q, total)
				return cacheValue{total: total}, nil
			})
			if err != nil {
				v.base.setError(err)
				return 0
			}
			if shared {
				statsFor(v.base.inst.Name).CacheHit.Add(1)
			}
			v.base.setError(nil)
			return val.total
		}
	}
	total, err := v.countWithQuery(q)
	if err != nil {
		v.base.setError(err)
		return 0
	}
	v.storeCountCache(q, total)
	v.base.setError(nil)
	return total
}

func (v *sqlView) countWithQuery(q Query) (int64, error) {
	plan, err := v.buildSQLPlan("count", q)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(v.name+".count.plan", ErrInvalidQuery, err)
	}
	start := time.Now()
	var total int64
	ctx, cancel := v.base.opContext(15 * time.Second)
	defer cancel()
	err = v.base.currentExec().QueryRowContext(ctx, plan.SQL, toInterfaces(plan.Params)...).Scan(&total)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return 0, wrapErr(v.name+".count.query", ErrInvalidQuery, v.base.classifySQLError(err))
	}
	v.base.logSlow(plan.SQL, plan.Params, start)
	statsFor(v.base.inst.Name).Queries.Add(1)
	return total, nil
}

func (v *sqlView) First(args ...Any) Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".first.parse", ErrInvalidQuery, err))
		return nil
	}
	q = v.mapQueryToStorage(q)
	q.Limit = 1
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".first.query", ErrInvalidQuery, err))
		return nil
	}
	if len(items) == 0 {
		v.base.setError(nil)
		return nil
	}
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		v.base.setError(nil)
		return items[0]
	}
	out, err := v.decode(items[0])
	v.base.setError(err)
	return out
}

func (v *sqlView) Query(args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".query.parse", ErrInvalidQuery, err))
		return nil
	}
	q = v.mapQueryToStorage(q)
	items, err := v.queryWithQuery(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".query.run", ErrInvalidQuery, err))
		return nil
	}
	if len(q.Aggs) > 0 || len(q.Group) > 0 {
		v.base.setError(nil)
		return items
	}
	out := make([]Map, 0, len(items))
	for _, item := range items {
		dec, err := v.decode(item)
		if err != nil {
			statsFor(v.base.inst.Name).Errors.Add(1)
			v.base.setError(wrapErr(v.name+".query.decode", ErrInvalidQuery, err))
			return nil
		}
		out = append(out, dec)
	}
	v.base.setError(nil)
	return out
}

func (v *sqlView) Aggregate(args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".agg.parse", ErrInvalidQuery, err))
		return nil
	}
	if len(q.Aggs) == 0 {
		// default aggregate: count
		q.Aggs = []Agg{{Alias: "$count", Op: "count", Field: "*"}}
	}
	q = v.mapQueryToStorage(q)
	items, err := v.queryWithQuery(q)
	v.base.setError(err)
	return items
}

func (v *sqlView) Scan(next ScanFunc, args ...Any) Res {
	return v.ScanN(0, next, args...)
}

func (v *sqlView) ScanN(limit int64, next ScanFunc, args ...Any) Res {
	if next == nil || limit < 0 {
		return infra.Fail
	}

	q, err := ParseQuery(args...)
	if err != nil {
		return infra.Fail.With(err.Error())
	}
	if limit > 0 {
		q.Limit = limit
	}
	q = v.mapQueryToStorage(q)
	batch := q.Batch
	if batch <= 0 {
		batch = v.base.scanBatchSize()
	}
	if batch > 0 {
		offset := q.Offset
		remain := q.Limit
		for {
			chunk := batch
			if remain > 0 && chunk > remain {
				chunk = remain
			}
			qq := q
			qq.Offset = offset
			qq.Limit = chunk
			items, err := v.queryWithQuery(qq)
			if err != nil {
				return infra.Fail.With(err.Error())
			}
			for _, item := range items {
				if res := next(item); res != nil && res.Fail() {
					return res
				}
			}
			if len(items) == 0 || int64(len(items)) < chunk {
				break
			}
			offset += int64(len(items))
			if remain > 0 {
				remain -= int64(len(items))
				if remain <= 0 {
					break
				}
			}
		}
		v.base.setError(nil)
		return infra.OK
	}

	res := v.streamWithQuery(q, next)
	if res == nil {
		return infra.OK
	}
	return res
}

func (v *sqlView) Slice(offset, limit int64, args ...Any) (int64, []Map) {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".limit.parse", ErrInvalidQuery, err))
		return 0, nil
	}
	q.Offset = offset
	q.Limit = limit
	q = v.ensureSliceSort(q)
	total := v.Count(args...)
	if v.base.Error() != nil {
		v.base.setError(wrapErr(v.name+".limit.count", ErrInvalidQuery, v.base.Error()))
		return 0, nil
	}
	items := v.Query(withQuery(args, q)...)
	if v.base.Error() != nil {
		v.base.setError(wrapErr(v.name+".limit.query", ErrInvalidQuery, v.base.Error()))
		return 0, nil
	}
	return total, items
}

func (v *sqlView) ensureSliceSort(q Query) Query {
	if len(q.Sort) > 0 {
		return q
	}
	key := strings.TrimSpace(v.key)
	if key == "" {
		return q
	}
	q.Sort = []Sort{{Field: key}}
	return q
}

func withQuery(args []Any, q Query) []Any {
	filtered := make([]Any, 0, len(args)+1)
	merged := Map{}
	for _, a := range args {
		if m, ok := a.(Map); ok {
			for k, v := range m {
				merged[k] = v
			}
		}
	}
	if q.Offset > 0 {
		merged[OptOffset] = q.Offset
	}
	if q.Limit > 0 {
		merged[OptLimit] = q.Limit
	}
	filtered = append(filtered, merged)
	return filtered
}

func (v *sqlView) Group(field string, args ...Any) []Map {
	q, err := ParseQuery(args...)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.parse", ErrInvalidQuery, err))
		return nil
	}
	q.Group = []string{field}
	q = v.mapQueryToStorage(q)
	builder := NewSQLBuilder(v.base.conn.Dialect())
	v.bindBuilder(builder, q)
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.from", ErrInvalidQuery, err))
		return nil
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.where", ErrInvalidQuery, err))
		return nil
	}
	groupField := quoteField(v.base.conn.Dialect(), v.base.storageField(field))
	sql := "SELECT " + groupField + " AS " + v.base.conn.Dialect().Quote(field) + ", COUNT(1) AS " + v.base.conn.Dialect().Quote("$count") +
		" FROM " + from + joins + " WHERE " + where + BuildGroupBy(q.Group, v.base.conn.Dialect())
	orderBy, err := v.buildOrderBy(q)
	if err != nil {
		v.base.setError(wrapErr(v.name+".group.order", ErrInvalidQuery, err))
		return nil
	}
	sql += orderBy
	start := time.Now()
	ctx, cancel := v.base.opContext(30 * time.Second)
	defer cancel()
	rows, err := v.base.currentExec().QueryContext(ctx, sql, toInterfaces(params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".group.query", ErrInvalidQuery, v.base.classifySQLError(err)))
		return nil
	}
	defer rows.Close()
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		v.base.setError(wrapErr(v.name+".group.scan", ErrInvalidQuery, v.base.classifySQLError(err)))
		return nil
	}
	statsFor(v.base.inst.Name).Queries.Add(1)
	v.base.logSlow(sql, params, start)
	v.base.setError(nil)
	return v.base.appMaps(items)
}

func (v *sqlView) queryWithQuery(q Query) ([]Map, error) {
	v.applyTrashScope(&q)
	if err := v.applyAfter(&q); err != nil {
		return nil, err
	}
	if items, ok := v.loadQueryCache(q); ok {
		statsFor(v.base.inst.Name).CacheHit.Add(1)
		return items, nil
	}
	if v.base.cacheEnabled() {
		key := v.queryCacheKey(q)
		if key != "" {
			statsFor(v.base.inst.Name).CacheMiss.Add(1)
			val, shared, err := cacheSingleflight(v.base.inst.Name, key, func() (cacheValue, error) {
				if items, ok := v.loadQueryCache(q); ok {
					return cacheValue{items: items}, nil
				}
				items, err := v.queryDB(q)
				if err != nil {
					return cacheValue{}, err
				}
				v.storeQueryCache(q, items)
				return cacheValue{items: items}, nil
			})
			if err != nil {
				return nil, err
			}
			if shared {
				statsFor(v.base.inst.Name).CacheHit.Add(1)
			}
			return cloneMaps(val.items), nil
		}
	}
	return v.queryDB(q)
}

func (v *sqlView) queryDB(q Query) ([]Map, error) {
	plan, err := v.buildSQLPlan("query", q)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	ctx, cancel := v.base.opContext(30 * time.Second)
	defer cancel()
	rows, err := v.base.currentExec().QueryContext(ctx, plan.SQL, toInterfaces(plan.Params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(v.name+".query.db", ErrInvalidQuery, v.base.classifySQLError(err))
	}
	defer rows.Close()
	v.base.logSlow(plan.SQL, plan.Params, start)
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(v.name+".query.scan", ErrInvalidQuery, v.base.classifySQLError(err))
	}
	items = v.base.appMaps(items)
	statsFor(v.base.inst.Name).Queries.Add(1)
	return items, nil
}

func (v *sqlView) streamWithQuery(q Query, next ScanFunc) Res {
	v.applyTrashScope(&q)
	if err := v.applyAfter(&q); err != nil {
		return infra.Fail.With(err.Error())
	}

	sqlText, params, err := v.streamSQLPlan(q)
	if err != nil {
		return infra.Fail.With(err.Error())
	}
	start := time.Now()
	ctx, cancel := v.base.opContext(30 * time.Second)
	defer cancel()
	rows, err := v.base.currentExec().QueryContext(ctx, sqlText, toInterfaces(params)...)
	if err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return infra.Fail.With(wrapErr(v.name+".range.query", ErrInvalidQuery, v.base.classifySQLError(err)).Error())
	}
	defer rows.Close()
	v.base.logSlow(sqlText, params, start)

	cols, err := rows.Columns()
	if err != nil {
		return infra.Fail.With(err.Error())
	}

	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return infra.Fail.With(err.Error())
		}
		item := Map{}
		for i, col := range cols {
			switch vv := values[i].(type) {
			case []byte:
				item[col] = string(vv)
			default:
				item[col] = vv
			}
		}
		item = v.base.appMap(item)

		if len(q.Aggs) == 0 && len(q.Group) == 0 {
			decoded, err := v.decode(item)
			if err != nil {
				return infra.Fail.With(err.Error())
			}
			item = decoded
		}

		if res := next(item); res != nil && res.Fail() {
			return res
		}
	}

	if err := rows.Err(); err != nil {
		statsFor(v.base.inst.Name).Errors.Add(1)
		return infra.Fail.With(wrapErr(v.name+".range.rows", ErrInvalidQuery, v.base.classifySQLError(err)).Error())
	}
	statsFor(v.base.inst.Name).Queries.Add(1)
	return infra.OK
}

func (v *sqlView) streamSQLPlan(q Query) (string, []Any, error) {
	plan, err := v.buildSQLPlan("stream", q)
	return plan.SQL, plan.Params, err
}

func (v *sqlView) buildSQLPlan(kind string, q Query) (sqlPlan, error) {
	if sql, params, ok := v.loadSQLPlan(kind, q); ok {
		return sqlPlan{SQL: sql, Params: params}, nil
	}
	plan, err := v.compileSQLPlan(kind, q)
	if err != nil {
		return sqlPlan{}, err
	}
	v.storeSQLPlan(kind, q, plan.SQL, plan.Params)
	return plan, nil
}

func (v *sqlView) compileSQLPlan(kind string, q Query) (sqlPlan, error) {
	builder := NewSQLBuilder(v.base.conn.Dialect())
	v.bindBuilder(builder, q)
	from, joins, err := v.buildFrom(q, builder)
	if err != nil {
		return sqlPlan{}, err
	}
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		return sqlPlan{}, err
	}
	if kind == "count" {
		sqlText := "SELECT COUNT(1) FROM " + from + joins + " WHERE " + where
		if len(q.Group) > 0 {
			sqlText = "SELECT COUNT(1) FROM (SELECT 1 FROM " + from + joins + " WHERE " + where + BuildGroupBy(q.Group, v.base.conn.Dialect()) + ") _g"
		}
		return sqlPlan{SQL: sqlText, Params: params}, nil
	}
	selectExpr, err := v.buildSelectExpr(q)
	if err != nil {
		return sqlPlan{}, err
	}
	sqlText := "SELECT " + selectExpr + " FROM " + from + joins + " WHERE " + where
	if len(q.Group) > 0 {
		sqlText += BuildGroupBy(q.Group, v.base.conn.Dialect())
		if q.Having != nil {
			havingSQL, err := builder.CompileExpr(q.Having)
			if err != nil {
				return sqlPlan{}, err
			}
			params = builder.Args()
			sqlText += " HAVING " + havingSQL
		}
	}
	orderBy, err := v.buildOrderBy(q)
	if err != nil {
		return sqlPlan{}, err
	}
	sqlText += orderBy
	sqlText += BuildLimitOffset(q, len(params)+1, v.base.conn.Dialect(), &params)
	return sqlPlan{SQL: sqlText, Params: params}, nil
}

func (v *sqlView) buildSelectExpr(q Query) (string, error) {
	if len(q.Aggs) > 0 {
		parts := make([]string, 0, len(q.Aggs)+len(q.Select))
		for _, agg := range q.Aggs {
			expr, err := compileAgg(v.base.conn.Dialect(), agg)
			if err != nil {
				return "", err
			}
			parts = append(parts, expr+" AS "+v.base.conn.Dialect().Quote(agg.Alias))
		}
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		return strings.Join(parts, ","), nil
	}
	if len(q.Select) > 0 {
		parts := make([]string, 0, len(q.Select))
		for _, field := range q.Select {
			parts = append(parts, quoteField(v.base.conn.Dialect(), field))
		}
		return strings.Join(parts, ","), nil
	}
	return "*", nil
}

func (v *sqlView) loadSQLPlan(kind string, q Query) (string, []Any, bool) {
	if v == nil || v.base == nil || v.base.planCacheCapacity() <= 0 {
		return "", nil, false
	}
	key := v.sqlPlanKey(kind, q)
	if key == "" {
		return "", nil, false
	}
	raw, ok := sqlPlanCache.Load(key)
	if !ok {
		return "", nil, false
	}
	plan, ok := raw.(*cachedSQLPlan)
	if !ok || strings.TrimSpace(plan.Plan.SQL) == "" {
		return "", nil, false
	}
	plan.Hits.Add(1)
	plan.UsedAt.Store(time.Now().UnixNano())
	return plan.Plan.SQL, cloneAnySlice(plan.Plan.Params), true
}

func (v *sqlView) storeSQLPlan(kind string, q Query, sqlText string, params []Any) {
	capacity := 0
	if v != nil && v.base != nil {
		capacity = v.base.planCacheCapacity()
	}
	if capacity <= 0 {
		return
	}
	key := v.sqlPlanKey(kind, q)
	if key == "" || strings.TrimSpace(sqlText) == "" {
		return
	}
	plan := &cachedSQLPlan{Plan: sqlPlan{SQL: sqlText, Params: cloneAnySlice(params)}}
	plan.Hits.Store(1)
	plan.UsedAt.Store(time.Now().UnixNano())
	actual, loaded := sqlPlanCache.LoadOrStore(key, plan)
	if loaded {
		if cached, ok := actual.(*cachedSQLPlan); ok {
			cached.Hits.Add(1)
			cached.UsedAt.Store(time.Now().UnixNano())
		}
		return
	}
	if !loaded {
		sqlPlanCacheCount.Add(1)
	}
	evictSQLPlans(capacity)
}

func evictSQLPlans(capacity int) {
	if capacity <= 0 {
		return
	}
	for sqlPlanCacheCount.Load() > int64(capacity) {
		key, ok := sqlPlanVictimKey()
		if !ok {
			return
		}
		if _, loaded := sqlPlanCache.LoadAndDelete(key); loaded {
			sqlPlanCacheCount.Add(-1)
			continue
		}
		if sqlPlanCacheCount.Load() <= int64(capacity) {
			return
		}
	}
}

func sqlPlanVictimKey() (Any, bool) {
	var victim Any
	var victimHits int64
	var victimUsedAt int64
	found := false
	sqlPlanCache.Range(func(k, v any) bool {
		plan, ok := v.(*cachedSQLPlan)
		if !ok || k == nil {
			return true
		}
		hits := plan.Hits.Load()
		usedAt := plan.UsedAt.Load()
		if !found || hits < victimHits || (hits == victimHits && usedAt < victimUsedAt) {
			victim = k
			victimHits = hits
			victimUsedAt = usedAt
			found = true
		}
		return true
	})
	return victim, found
}

func (v *sqlView) sqlPlanKey(kind string, q Query) string {
	if v == nil || v.base == nil || v.base.conn == nil {
		return ""
	}
	d := v.base.conn.Dialect()
	instName := ""
	if v.base.inst != nil {
		instName = v.base.inst.Name
	}
	parts := []string{
		"kind=" + strings.TrimSpace(kind),
		"dialect=" + strings.ToLower(strings.TrimSpace(d.Name())),
		fmt.Sprintf("ilike=%t", d.SupportsILike()),
		fmt.Sprintf("returning=%t", d.SupportsReturning()),
		"instance=" + strings.TrimSpace(instName),
		"name=" + strings.TrimSpace(v.name),
		"schema=" + strings.TrimSpace(v.schema),
		"source=" + strings.TrimSpace(v.source),
		"key=" + strings.TrimSpace(v.key),
		fmt.Sprintf("mapping=%t", v.base.fieldMappingEnabled()),
		"fields=" + fieldsPlanSignature(v.fields),
		"query=" + QuerySignature(q),
	}
	return strings.Join(parts, "|")
}

func cloneAnySlice(in []Any) []Any {
	if len(in) == 0 {
		return nil
	}
	out := make([]Any, len(in))
	copy(out, in)
	return out
}

func fieldsPlanSignature(fields Vars) string {
	if len(fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		cfg := fields[k]
		flags := make([]string, 0, len(cfg.Setting))
		for key, value := range cfg.Setting {
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "array", "multiple":
				flags = append(flags, key+"="+stableAnySignature(value))
			}
		}
		sort.Strings(flags)
		parts = append(parts, strings.Join([]string{
			k,
			strings.ToLower(strings.TrimSpace(cfg.Type)),
			strings.Join(flags, ","),
			fieldsPlanSignature(cfg.Children),
		}, ":"))
	}
	return strings.Join(parts, ";")
}

func (v *sqlView) bindBuilder(builder *SQLBuilder, q Query) {
	if builder == nil {
		return
	}
	builder.isAlias = func(name string) bool {
		return v.isKnownAlias(name, q)
	}
	builder.isJSONField = v.isJSONField
	builder.isArrayField = v.isArrayField
	builder.toStorage = v.base.storageField
	builder.bindField = v.bindFieldValue
}

func (v *sqlView) bindFieldValue(name string, value Any) Any {
	name = strings.TrimSpace(name)
	if name == "" || len(v.fields) == 0 {
		return value
	}
	candidates := []string{name}
	if v.base.fieldMappingEnabled() {
		candidates = append(candidates, v.base.appField(name), v.base.storageField(name))
	}
	for _, one := range candidates {
		cfg, ok := lookupField(v.fields, one)
		if !ok {
			continue
		}
		if out, yes := bindStructuredValue(v.base.conn.Dialect(), cfg, value); yes {
			return out
		}
	}
	return value
}

func (v *sqlView) buildOrderBy(q Query) (string, error) {
	if len(q.Sort) == 0 {
		return "", nil
	}
	d := v.base.conn.Dialect()
	dn := strings.ToLower(d.Name())
	parts := make([]string, 0, len(q.Sort))
	for _, s := range q.Sort {
		field := strings.TrimSpace(s.Field)
		if field == "" {
			continue
		}
		expr := ""
		if strings.Contains(field, ".") {
			top := strings.SplitN(field, ".", 2)[0]
			if v.isKnownAlias(top, q) {
				expr = quoteField(d, field)
			} else if v.isJSONField(top) {
				path := strings.Split(field, ".")[1:]
				j, err := v.buildJSONSortExpr(top, path, dn)
				if err != nil {
					return "", err
				}
				expr = j
			} else {
				return "", fmt.Errorf("ambiguous sort field %s: neither alias.field nor jsonPath on JSON field", field)
			}
		} else {
			expr = quoteField(d, field)
		}
		if s.Desc {
			parts = append(parts, expr+" DESC")
		} else {
			parts = append(parts, expr+" ASC")
		}
	}
	if len(parts) == 0 {
		return "", nil
	}
	return " ORDER BY " + strings.Join(parts, ","), nil
}

func (v *sqlView) isKnownAlias(name string, q Query) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	if strings.EqualFold(name, v.name) || strings.EqualFold(name, v.source) {
		return true
	}
	for _, j := range q.Joins {
		alias := strings.TrimSpace(j.Alias)
		if alias == "" {
			alias = strings.TrimSpace(j.From)
		}
		if strings.EqualFold(name, alias) || strings.EqualFold(name, strings.TrimSpace(j.From)) {
			return true
		}
	}
	return false
}

func (v *sqlView) isJSONField(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	candidates := []string{name}
	if v.base.fieldMappingEnabled() {
		candidates = append(candidates, v.base.appField(name), v.base.storageField(name))
	}
	for _, one := range candidates {
		cfg, ok := v.fields[one]
		if !ok {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(cfg.Type))
		if t == "json" || t == "jsonb" || strings.HasPrefix(t, "map") {
			return true
		}
	}
	return false
}

func (v *sqlView) isArrayField(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	candidates := []string{name}
	if v.base.fieldMappingEnabled() {
		candidates = append(candidates, v.base.appField(name), v.base.storageField(name))
	}
	for _, one := range candidates {
		cfg, ok := v.fields[one]
		if !ok {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(cfg.Type))
		if strings.HasPrefix(t, "[") || strings.HasPrefix(t, "array") {
			return true
		}
		if cfg.Setting == nil {
			continue
		}
		for _, key := range []string{"array", "multiple"} {
			if raw, ok := cfg.Setting[key]; ok {
				if on, yes := parseBool(raw); yes && on {
					return true
				}
			}
		}
	}
	return false
}

func (v *sqlView) buildJSONSortExpr(field string, path []string, dialect string) (string, error) {
	if len(path) == 0 {
		return quoteField(v.base.conn.Dialect(), field), nil
	}
	d := v.base.conn.Dialect()
	base := d.Quote(v.base.storageField(field))
	switch {
	case dialect == "pgsql" || dialect == "postgres":
		if len(path) == 1 {
			return fmt.Sprintf("(%s->>'%s')", base, path[0]), nil
		}
		return fmt.Sprintf("(%s#>>'{%s}')", base, strings.Join(path, ",")), nil
	case dialect == "mysql":
		return fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(%s, '$.%s'))", base, strings.Join(path, ".")), nil
	case dialect == "sqlite":
		return fmt.Sprintf("json_extract(%s, '$.%s')", base, strings.Join(path, ".")), nil
	default:
		return "", fmt.Errorf("dialect %s does not support json sort path %s.%s", dialect, field, strings.Join(path, "."))
	}
}

func (v *sqlView) buildFrom(q Query, builder *SQLBuilder) (string, string, error) {
	from := v.base.sourceExpr(v.schema, v.source)
	if len(q.Joins) == 0 {
		return from, "", nil
	}
	parts := make([]string, 0, len(q.Joins))
	aliasSet := map[string]struct{}{}
	aliasSet[strings.ToLower(strings.TrimSpace(v.source))] = struct{}{}
	aliasSet[strings.ToLower(strings.TrimSpace(v.name))] = struct{}{}
	for _, j := range q.Joins {
		alias := j.Alias
		if alias == "" {
			alias = j.From
		}
		alias = strings.TrimSpace(alias)
		if alias == "" {
			return "", "", fmt.Errorf("join %s invalid alias", j.From)
		}
		if _, exists := aliasSet[strings.ToLower(alias)]; exists {
			return "", "", fmt.Errorf("join alias duplicated: %s", alias)
		}
		aliasSet[strings.ToLower(alias)] = struct{}{}
		joinType := strings.ToUpper(strings.TrimSpace(j.Type))
		if joinType == "" {
			joinType = "LEFT"
		}
		switch joinType {
		case "LEFT", "RIGHT", "INNER", "FULL":
		default:
			return "", "", fmt.Errorf("join %s invalid type: %s", j.From, joinType)
		}
		on := ""
		if j.LocalField != "" && j.ForeignField != "" {
			on = fmt.Sprintf("%s = %s", quoteField(v.base.conn.Dialect(), j.LocalField), quoteField(v.base.conn.Dialect(), j.ForeignField))
		} else if j.On != nil {
			onSQL, err := builder.CompileExpr(j.On)
			if err != nil {
				return "", "", err
			}
			on = onSQL
		}
		if on == "" {
			return "", "", fmt.Errorf("join %s missing ON condition", j.From)
		}
		part := fmt.Sprintf(" %s JOIN %s", joinType, v.base.sourceExpr("", j.From))
		if alias != j.From {
			part += " AS " + v.base.conn.Dialect().Quote(alias)
		}
		part += " ON " + on
		parts = append(parts, part)
	}
	return from, strings.Join(parts, ""), nil
}

func (v *sqlView) decode(item Map) (Map, error) {
	item = v.base.appMap(item)
	item = v.decodeStructuredValues(item)
	if len(v.fields) == 0 {
		return item, nil
	}
	out := Map{}
	res := infra.Mapping(v.fields, item, out, false, true)
	if res != nil && res.Fail() {
		return nil, fmt.Errorf("decode %s failed: %s", v.name, res.Error())
	}
	return out, nil
}

func (v *sqlView) decodeStructuredValues(item Map) Map {
	if len(item) == 0 || len(v.fields) == 0 {
		return item
	}
	var d Dialect
	if v.base != nil && v.base.conn != nil {
		d = v.base.conn.Dialect()
	}
	out := Map{}
	for key, value := range item {
		cfg, ok := lookupField(v.fields, key)
		if !ok {
			out[key] = value
			continue
		}
		out[key] = decodeStructuredFieldValue(d, cfg, value)
	}
	return out
}

func decodeStructuredFieldValue(d Dialect, cfg Var, value Any) Any {
	if out, ok := decodeStructuredValue(d, cfg, value); ok {
		return out
	}
	return value
}

func normalizeParsedArrayValue(kind string, value Any) Any {
	items, ok := value.([]Any)
	if !ok {
		switch {
		case strings.Contains(kind, "uint"):
			if n, ok := toInt64Value(value); ok {
				if n < 0 {
					n = 0
				}
				return n
			}
		case strings.Contains(kind, "int"):
			if n, ok := toInt64Value(value); ok {
				return n
			}
		case strings.Contains(kind, "float") || strings.Contains(kind, "double") || strings.Contains(kind, "decimal") || strings.Contains(kind, "number"):
			if n, ok := toFloat64Value(value); ok {
				return n
			}
		case strings.Contains(kind, "bool"):
			if b, ok := value.(bool); ok {
				return b
			}
			if s, ok := value.(string); ok {
				if b, yes := parseBool(s); yes {
					return b
				}
			}
		case strings.Contains(kind, "string"), strings.Contains(kind, "text"):
			if s, ok := value.(string); ok {
				return s
			}
		}
		return value
	}
	normalized := make([]Any, 0, len(items))
	for _, item := range items {
		normalized = append(normalized, normalizeParsedArrayValue(kind, item))
	}
	if typed, ok := homogenizeTypedSlice(normalized); ok {
		return typed
	}
	return normalized
}

func homogenizeTypedSlice(items []Any) (Any, bool) {
	if len(items) == 0 {
		return []Any{}, true
	}
	if allNil(items) {
		return items, true
	}
	var elemType reflect.Type
	for _, item := range items {
		if item == nil {
			return items, false
		}
		t := reflect.TypeOf(item)
		if elemType == nil {
			elemType = t
			continue
		}
		if t != elemType {
			return items, false
		}
	}
	if elemType == nil {
		return items, true
	}
	out := reflect.MakeSlice(reflect.SliceOf(elemType), len(items), len(items))
	for i, item := range items {
		out.Index(i).Set(reflect.ValueOf(item))
	}
	return out.Interface(), true
}

func allNil(items []Any) bool {
	for _, item := range items {
		if item != nil {
			return false
		}
	}
	return true
}

func isArrayVar(cfg Var) bool {
	kind := strings.ToLower(strings.TrimSpace(cfg.Type))
	if strings.HasPrefix(kind, "[") || strings.HasPrefix(kind, "array") {
		return true
	}
	if cfg.Setting == nil {
		return false
	}
	for _, key := range []string{"array", "multiple"} {
		if raw, ok := cfg.Setting[key]; ok {
			if on, yes := parseBool(raw); yes && on {
				return true
			}
		}
	}
	return false
}

func toInt64Slice(value Any, nonNegative bool) ([]int64, bool) {
	items, ok := value.([]Any)
	if !ok {
		if raw, ok := value.([]any); ok {
			items = raw
		} else {
			return nil, false
		}
	}
	out := make([]int64, 0, len(items))
	for _, one := range items {
		n, ok := toInt64Value(one)
		if !ok {
			return nil, false
		}
		if nonNegative && n < 0 {
			n = 0
		}
		out = append(out, n)
	}
	return out, true
}

func toFloat64Slice(value Any) ([]float64, bool) {
	items, ok := value.([]Any)
	if !ok {
		if raw, ok := value.([]any); ok {
			items = raw
		} else {
			return nil, false
		}
	}
	out := make([]float64, 0, len(items))
	for _, one := range items {
		n, ok := toFloat64Value(one)
		if !ok {
			return nil, false
		}
		out = append(out, n)
	}
	return out, true
}

func toInt64Value(value Any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

func toFloat64Value(value Any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseFloat(s, 64)
		return n, err == nil
	}
	return 0, false
}

func (v *sqlView) loadQueryCache(q Query) ([]Map, bool) {
	if !v.base.cacheEnabled() {
		return nil, false
	}
	key := v.queryCacheKey(q)
	raw, ok := cacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return nil, false
	}
	cv, ok := raw.(cacheValue)
	if !ok {
		return nil, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		cacheDelete(v.base.inst.Name, key)
		return nil, false
	}
	return cloneMaps(cv.items), true
}

func (v *sqlView) storeQueryCache(q Query, items []Map) {
	if !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	key := v.queryCacheKey(q)
	tables := v.cacheTables(q)
	cacheStoreWithCap(v.base.inst.Name, key, cacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		items:    cloneMaps(items),
		total:    -1,
	}, v.base.cacheCapacity(), tables)
}

func (v *sqlView) loadCountCache(q Query) (int64, bool) {
	if !v.base.cacheEnabled() {
		return 0, false
	}
	key := v.countCacheKey(q)
	raw, ok := cacheMap(v.base.inst.Name).Load(key)
	if !ok {
		return 0, false
	}
	cv, ok := raw.(cacheValue)
	if !ok {
		return 0, false
	}
	if cv.expireAt > 0 && time.Now().UnixNano() > cv.expireAt {
		cacheDelete(v.base.inst.Name, key)
		return 0, false
	}
	return cv.total, true
}

func (v *sqlView) storeCountCache(q Query, total int64) {
	if !v.base.cacheEnabled() {
		return
	}
	ttl := v.base.cacheTTL()
	if ttl <= 0 {
		return
	}
	key := v.countCacheKey(q)
	tables := v.cacheTables(q)
	cacheStoreWithCap(v.base.inst.Name, key, cacheValue{
		expireAt: time.Now().Add(ttl).UnixNano(),
		total:    total,
	}, v.base.cacheCapacity(), tables)
}

func (v *sqlView) queryCacheKey(q Query) string {
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	return fmt.Sprintf("q:%s:%s", token, makeCacheKey(v.base, v.name, q))
}

func (v *sqlView) countCacheKey(q Query) string {
	token := cacheToken(v.base.inst.Name, v.cacheTables(q))
	return fmt.Sprintf("c:%s:%s", token, makeCacheKey(v.base, v.name, q))
}

func (v *sqlView) cacheTables(q Query) []string {
	out := make([]string, 0, len(q.Joins)+1)
	out = append(out, v.source)
	for _, join := range q.Joins {
		if strings.TrimSpace(join.From) != "" {
			out = append(out, join.From)
		}
	}
	return out
}

func (v *sqlView) applyAfter(q *Query) error {
	if len(q.After) == 0 {
		return nil
	}
	if len(q.Sort) == 0 {
		return fmt.Errorf("$after requires $sort")
	}
	sf := q.Sort[0]
	value, ok := q.After[sf.Field]
	if !ok {
		value, ok = q.After["$value"]
	}
	if !ok {
		return fmt.Errorf("$after missing value for sort field %s", sf.Field)
	}
	op := OpGt
	if sf.Desc {
		op = OpLt
	}
	afterExpr := CmpExpr{Field: sf.Field, Op: op, Value: value}
	if q.Filter == nil {
		q.Filter = afterExpr
		return nil
	}
	if _, ok := q.Filter.(TrueExpr); ok {
		q.Filter = afterExpr
		return nil
	}
	q.Filter = AndExpr{Items: []Expr{q.Filter, afterExpr}}
	return nil
}

func compileAgg(d Dialect, agg Agg) (string, error) {
	op := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(agg.Op)), "$")
	field := strings.TrimSpace(agg.Field)
	if field == "" {
		field = "*"
	}
	var fn string
	switch op {
	case "count":
		fn = "COUNT"
		if field == "*" {
			return fn + "(1)", nil
		}
	case "sum":
		fn = "SUM"
	case "avg":
		fn = "AVG"
	case "min":
		fn = "MIN"
	case "max":
		fn = "MAX"
	default:
		return "", fmt.Errorf("unsupported agg op %s", agg.Op)
	}
	return fn + "(" + quoteField(d, field) + ")", nil
}
