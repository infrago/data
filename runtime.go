package data

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infrago/infra"
	. "github.com/infrago/base"
)

type moduleStats struct {
	Queries    atomic.Int64
	Writes     atomic.Int64
	Errors     atomic.Int64
	Slow       atomic.Int64
	CacheHit   atomic.Int64
	SlowMs     atomic.Int64
	B1         atomic.Int64
	B2         atomic.Int64
	B3         atomic.Int64
	B4         atomic.Int64
	B5         atomic.Int64
	B6         atomic.Int64
	B7         atomic.Int64
	ChangeIn   atomic.Int64
	ChangeDrop atomic.Int64
	ChangeFail atomic.Int64
}

type cacheValue struct {
	expireAt int64
	items    []Map
	total    int64
}

type Stats struct {
	Queries    int64   `json:"queries"`
	Writes     int64   `json:"writes"`
	Errors     int64   `json:"errors"`
	Slow       int64   `json:"slow"`
	CacheHit   int64   `json:"cacheHit"`
	CacheRate  float64 `json:"cacheRate"`
	SlowAvgMs  int64   `json:"slowAvgMs"`
	SlowP50Ms  int64   `json:"slowP50Ms"`
	SlowP95Ms  int64   `json:"slowP95Ms"`
	ChangeIn   int64   `json:"changeIn"`
	ChangeDrop int64   `json:"changeDrop"`
	ChangeFail int64   `json:"changeFail"`
}

var (
	statsRegistry sync.Map
	cacheRegistry sync.Map
	cacheVersion  sync.Map
	cacheTableVer sync.Map
	cacheDepend   sync.Map
	cacheKeyDeps  sync.Map
	cacheCount    sync.Map
	cacheSyncOnce sync.Once
)

const cacheInvalidateTopic = "_data.cache.invalidate"

func statsFor(name string) *moduleStats {
	if name == "" {
		name = "default"
	}
	if v, ok := statsRegistry.Load(name); ok {
		return v.(*moduleStats)
	}
	s := &moduleStats{}
	actual, _ := statsRegistry.LoadOrStore(name, s)
	return actual.(*moduleStats)
}

func (m *Module) Stats(names ...string) Stats {
	name := "default"
	if len(names) > 0 && strings.TrimSpace(names[0]) != "" {
		name = names[0]
	}
	s := statsFor(name)
	queries := s.Queries.Load()
	cacheHit := s.CacheHit.Load()
	slow := s.Slow.Load()
	slowMs := s.SlowMs.Load()
	return Stats{
		Queries:    queries,
		Writes:     s.Writes.Load(),
		Errors:     s.Errors.Load(),
		Slow:       slow,
		CacheHit:   cacheHit,
		CacheRate:  ratio(cacheHit, queries),
		SlowAvgMs:  avg(slowMs, slow),
		SlowP50Ms:  slowPercentile(s, 0.50),
		SlowP95Ms:  slowPercentile(s, 0.95),
		ChangeIn:   s.ChangeIn.Load(),
		ChangeDrop: s.ChangeDrop.Load(),
		ChangeFail: s.ChangeFail.Load(),
	}
}

func ratio(a, b int64) float64 {
	if b <= 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func avg(sum, n int64) int64 {
	if n <= 0 {
		return 0
	}
	return sum / n
}

func observeSlowCost(name string, cost time.Duration) {
	ms := cost.Milliseconds()
	s := statsFor(name)
	s.SlowMs.Add(ms)
	switch {
	case ms <= 50:
		s.B1.Add(1)
	case ms <= 100:
		s.B2.Add(1)
	case ms <= 200:
		s.B3.Add(1)
	case ms <= 500:
		s.B4.Add(1)
	case ms <= 1000:
		s.B5.Add(1)
	case ms <= 3000:
		s.B6.Add(1)
	default:
		s.B7.Add(1)
	}
}

func slowPercentile(s *moduleStats, p float64) int64 {
	total := s.Slow.Load()
	if total <= 0 {
		return 0
	}
	target := int64(float64(total) * p)
	if target <= 0 {
		target = 1
	}
	buckets := []struct {
		n   int64
		max int64
	}{
		{n: s.B1.Load(), max: 50},
		{n: s.B2.Load(), max: 100},
		{n: s.B3.Load(), max: 200},
		{n: s.B4.Load(), max: 500},
		{n: s.B5.Load(), max: 1000},
		{n: s.B6.Load(), max: 3000},
		{n: s.B7.Load(), max: 10000},
	}
	acc := int64(0)
	for _, b := range buckets {
		acc += b.n
		if acc >= target {
			return b.max
		}
	}
	return 10000
}

func cacheMap(name string) *sync.Map {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheRegistry.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := cacheRegistry.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func cacheCountPtr(name string) *atomic.Int64 {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheCount.Load(name); ok {
		return v.(*atomic.Int64)
	}
	p := &atomic.Int64{}
	actual, _ := cacheCount.LoadOrStore(name, p)
	return actual.(*atomic.Int64)
}

func cacheVersionPtr(name string) *atomic.Uint64 {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheVersion.Load(name); ok {
		return v.(*atomic.Uint64)
	}
	p := &atomic.Uint64{}
	actual, _ := cacheVersion.LoadOrStore(name, p)
	return actual.(*atomic.Uint64)
}

func cacheVersionGet(name string) uint64 {
	return cacheVersionPtr(name).Load()
}

func cacheVersionBump(name string) {
	cacheVersionPtr(name).Add(1)
}

func cacheTableMap(name string) *sync.Map {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheTableVer.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := cacheTableVer.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func cacheTableVersionPtr(name, table string) *atomic.Uint64 {
	table = normalizeCacheTable(table)
	tables := cacheTableMap(name)
	if v, ok := tables.Load(table); ok {
		return v.(*atomic.Uint64)
	}
	p := &atomic.Uint64{}
	actual, _ := tables.LoadOrStore(table, p)
	return actual.(*atomic.Uint64)
}

func cacheTableVersionGet(name, table string) uint64 {
	return cacheTableVersionPtr(name, table).Load()
}

func cacheTouchTable(name, table string) {
	key := normalizeCacheTable(table)
	ver := cacheTouchTableLocal(name, key)
	cacheBroadcastInvalidate(name, key, ver)
}

func cacheTouchTableLocal(name, table string) uint64 {
	key := normalizeCacheTable(table)
	ver := cacheTableVersionPtr(name, key).Add(1)
	cacheInvalidateTable(name, key)
	return ver
}

func cacheSetTableVersion(name, table string, target uint64) uint64 {
	ptr := cacheTableVersionPtr(name, table)
	for {
		cur := ptr.Load()
		if cur >= target {
			return cur
		}
		if ptr.CompareAndSwap(cur, target) {
			return target
		}
	}
}

func cacheBroadcastInvalidate(name, table string, ver uint64) {
	_ = infra.Broadcast(cacheInvalidateTopic, Map{
		"base":  name,
		"table": table,
		"ver":   ver,
		"ts":    time.Now().UnixMilli(),
	})
}

func normalizeCacheTable(table string) string {
	t := strings.TrimSpace(strings.ToLower(table))
	if t == "" {
		return "*"
	}
	return t
}

func cacheToken(name string, tables []string) string {
	uniq := make(map[string]struct{}, len(tables))
	list := make([]string, 0, len(tables))
	for _, table := range tables {
		key := normalizeCacheTable(table)
		if _, ok := uniq[key]; ok {
			continue
		}
		uniq[key] = struct{}{}
		list = append(list, key)
	}
	sort.Strings(list)
	parts := make([]string, 0, len(list)+1)
	parts = append(parts, fmt.Sprintf("g:%d", cacheVersionGet(name)))
	for _, key := range list {
		parts = append(parts, fmt.Sprintf("%s:%d", key, cacheTableVersionGet(name, key)))
	}
	return strings.Join(parts, "|")
}

func cacheDependMap(name string) *sync.Map {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheDepend.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := cacheDepend.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func cacheTableKeys(name, table string) *sync.Map {
	table = normalizeCacheTable(table)
	dep := cacheDependMap(name)
	if v, ok := dep.Load(table); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := dep.LoadOrStore(table, m)
	return actual.(*sync.Map)
}

func cacheTrackKey(name, key string, tables []string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	cacheUntrackKey(name, key)
	seen := map[string]struct{}{}
	deps := make([]string, 0, len(tables))
	for _, table := range tables {
		t := normalizeCacheTable(table)
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		deps = append(deps, t)
		cacheTableKeys(name, t).Store(key, struct{}{})
	}
	cacheKeyDepMap(name).Store(key, deps)
}

func cacheKeyDepMap(name string) *sync.Map {
	if name == "" {
		name = "default"
	}
	if v, ok := cacheKeyDeps.Load(name); ok {
		return v.(*sync.Map)
	}
	m := &sync.Map{}
	actual, _ := cacheKeyDeps.LoadOrStore(name, m)
	return actual.(*sync.Map)
}

func cacheUntrackKey(name, key string) {
	depMap := cacheKeyDepMap(name)
	raw, ok := depMap.Load(key)
	if !ok {
		return
	}
	depMap.Delete(key)
	list, ok := raw.([]string)
	if !ok {
		return
	}
	for _, table := range list {
		cacheTableKeys(name, table).Delete(key)
	}
}

func cacheDelete(name, key string) {
	if _, ok := cacheMap(name).LoadAndDelete(key); ok {
		cacheCountPtr(name).Add(-1)
	}
	cacheUntrackKey(name, key)
}

func cacheStoreWithCap(name, key string, val cacheValue, capacity int, tables []string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	if _, loaded := cacheMap(name).Load(key); !loaded {
		cacheCountPtr(name).Add(1)
	}
	cacheMap(name).Store(key, val)
	cacheTrackKey(name, key, tables)
	if capacity > 0 {
		cacheEvict(name, capacity)
	}
}

func cacheEvict(name string, capacity int) {
	for cacheCountPtr(name).Load() > int64(capacity) {
		removed := false
		cacheMap(name).Range(func(k, v any) bool {
			key, ok := k.(string)
			if !ok || key == "" {
				return true
			}
			cacheDelete(name, key)
			removed = true
			return false
		})
		if !removed {
			break
		}
	}
}

func cacheInvalidateTable(name, table string) {
	keys := cacheTableKeys(name, table)
	keys.Range(func(k, _ any) bool {
		if key, ok := k.(string); ok && key != "" {
			cacheDelete(name, key)
		}
		keys.Delete(k)
		return true
	})
}

func registerCacheSyncService(h infra.Host) {
	cacheSyncOnce.Do(func() {
		service := infra.Service{
			Name: "数据缓存失效同步",
			Desc: "内部服务：同步数据查询缓存按表失效事件",
			Action: func(ctx *infra.Context) {
				baseName, _ := ctx.Value["base"].(string)
				table, _ := ctx.Value["table"].(string)
				if strings.TrimSpace(baseName) == "" || strings.TrimSpace(table) == "" {
					return
				}
				ver := parseUint64Any(ctx.Value["ver"])
				if ver > 0 {
					cacheSetTableVersion(baseName, table, ver)
				}
				cacheInvalidateTable(baseName, table)
			},
		}
		if h != nil {
			h.RegisterLocal(cacheInvalidateTopic, service)
			return
		}
		infra.Register(cacheInvalidateTopic, service)
	})
}

func parseUint64Any(v Any) uint64 {
	switch vv := v.(type) {
	case uint64:
		return vv
	case int64:
		if vv < 0 {
			return 0
		}
		return uint64(vv)
	case int:
		if vv < 0 {
			return 0
		}
		return uint64(vv)
	case float64:
		if vv < 0 {
			return 0
		}
		return uint64(vv)
	case string:
		n, _ := strconv.ParseUint(strings.TrimSpace(vv), 10, 64)
		return n
	default:
		return 0
	}
}

func CacheToken(name string, tables []string) string {
	return cacheToken(name, tables)
}

func TouchTableCache(name, table string) uint64 {
	key := normalizeCacheTable(table)
	ver := cacheTouchTableLocal(name, key)
	cacheBroadcastInvalidate(name, key, ver)
	return ver
}

func cloneMaps(items []Map) []Map {
	out := make([]Map, 0, len(items))
	for _, item := range items {
		one := Map{}
		for k, v := range item {
			one[k] = v
		}
		out = append(out, one)
	}
	return out
}

func makeCacheKey(base *sqlBase, scope string, q Query) string {
	return scope + "|" + QuerySignature(q)
}

func (b *sqlBase) cacheEnabled() bool {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return false
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return false
	}
	switch vv := raw.(type) {
	case bool:
		return vv
	case int:
		return vv > 0
	case int64:
		return vv > 0
	case string:
		on, err := strconv.ParseBool(strings.TrimSpace(vv))
		return err == nil && on
	case Map:
		if e, ok := vv["enable"]; ok {
			on, yes := parseBool(e)
			return yes && on
		}
		return true
	default:
		return false
	}
}

func (b *sqlBase) cacheTTL() time.Duration {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return 0
	}
	switch vv := raw.(type) {
	case Map:
		if d, ok := vv["ttl"]; ok {
			switch dt := d.(type) {
			case string:
				if parsed, err := time.ParseDuration(dt); err == nil && parsed > 0 {
					return parsed
				}
			case int:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			case int64:
				if dt > 0 {
					return time.Second * time.Duration(dt)
				}
			}
		}
	}
	return 3 * time.Second
}

func (b *sqlBase) cacheCapacity() int {
	if b == nil || b.inst == nil || b.inst.Config.Setting == nil {
		return 0
	}
	raw, ok := b.inst.Config.Setting["cache"]
	if !ok {
		return 0
	}
	if vv, ok := raw.(Map); ok {
		for _, key := range []string{"capacity", "cap", "max"} {
			if c, ok := vv[key]; ok {
				if n, yes := parseIntAny(c); yes && n > 0 {
					return n
				}
			}
		}
	}
	return 0
}
