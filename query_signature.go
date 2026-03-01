package data

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	. "github.com/infrago/base"
)

func QuerySignature(q Query) string {
	parts := make([]string, 0, 16)
	parts = append(parts, "select="+strings.Join(q.Select, ","))
	parts = append(parts, "sort="+sortSignature(q.Sort))
	parts = append(parts, "group="+strings.Join(q.Group, ","))
	parts = append(parts, "agg="+aggSignature(q.Aggs))
	parts = append(parts, "join="+joinSignature(q.Joins))
	parts = append(parts, "filter="+exprSignature(q.Filter))
	parts = append(parts, "having="+exprSignature(q.Having))
	parts = append(parts, "after="+stableAnySignature(q.After))
	parts = append(parts, fmt.Sprintf("limit=%d", q.Limit))
	parts = append(parts, fmt.Sprintf("offset=%d", q.Offset))
	parts = append(parts, fmt.Sprintf("batch=%d", q.Batch))
	parts = append(parts, fmt.Sprintf("withCount=%t", q.WithCount))
	parts = append(parts, fmt.Sprintf("unsafe=%t", q.Unsafe))
	parts = append(parts, "rawWhere="+strings.TrimSpace(q.RawWhere))
	parts = append(parts, "rawParams="+stableAnySignature(q.RawParams))
	return strings.Join(parts, "|")
}

func sortSignature(in []Sort) string {
	parts := make([]string, 0, len(in))
	for _, s := range in {
		dir := "asc"
		if s.Desc {
			dir = "desc"
		}
		parts = append(parts, strings.TrimSpace(s.Field)+":"+dir)
	}
	return strings.Join(parts, ",")
}

func aggSignature(in []Agg) string {
	parts := make([]string, 0, len(in))
	for _, a := range in {
		parts = append(parts, strings.TrimSpace(a.Alias)+":"+strings.TrimSpace(a.Op)+":"+strings.TrimSpace(a.Field))
	}
	return strings.Join(parts, ",")
}

func joinSignature(in []Join) string {
	parts := make([]string, 0, len(in))
	for _, j := range in {
		parts = append(parts, strings.Join([]string{
			strings.TrimSpace(j.From),
			strings.TrimSpace(j.Alias),
			strings.TrimSpace(j.Type),
			strings.TrimSpace(j.LocalField),
			strings.TrimSpace(j.ForeignField),
			exprSignature(j.On),
		}, ":"))
	}
	return strings.Join(parts, ",")
}

func exprSignature(expr Expr) string {
	switch e := expr.(type) {
	case nil:
		return "nil"
	case TrueExpr:
		return "true"
	case RawExpr:
		return "raw(" + strings.TrimSpace(e.SQL) + "," + stableAnySignature(e.Params) + ")"
	case CmpExpr:
		return "cmp(" + strings.TrimSpace(e.Field) + "," + strings.TrimSpace(e.Op) + "," + stableAnySignature(e.Value) + ")"
	case ExistsExpr:
		return fmt.Sprintf("exists(%s,%t)", strings.TrimSpace(e.Field), e.Yes)
	case NullExpr:
		return fmt.Sprintf("null(%s,%t)", strings.TrimSpace(e.Field), e.Yes)
	case NotExpr:
		return "not(" + exprSignature(e.Item) + ")"
	case AndExpr:
		items := make([]string, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, exprSignature(one))
		}
		sort.Strings(items)
		return "and(" + strings.Join(items, ",") + ")"
	case OrExpr:
		items := make([]string, 0, len(e.Items))
		for _, one := range e.Items {
			items = append(items, exprSignature(one))
		}
		sort.Strings(items)
		return "or(" + strings.Join(items, ",") + ")"
	default:
		return "unknown(" + stableAnySignature(e) + ")"
	}
}

func stableAnySignature(v Any) string {
	switch vv := v.(type) {
	case nil:
		return "null"
	case string:
		return "s:" + vv
	case bool:
		if vv {
			return "b:1"
		}
		return "b:0"
	case int:
		return fmt.Sprintf("i:%d", vv)
	case int64:
		return fmt.Sprintf("i64:%d", vv)
	case float64:
		return fmt.Sprintf("f:%g", vv)
	case time.Time:
		return "t:" + vv.UTC().Format(time.RFC3339Nano)
	case []Any:
		parts := make([]string, 0, len(vv))
		for _, one := range vv {
			parts = append(parts, stableAnySignature(one))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case []string:
		parts := make([]string, 0, len(vv))
		for _, one := range vv {
			parts = append(parts, "s:"+one)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case Map:
		keys := make([]string, 0, len(vv))
		for k := range vv {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, k+":"+stableAnySignature(vv[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	}

	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return "null"
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		parts := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			parts = append(parts, stableAnySignature(rv.Index(i).Interface()))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case reflect.Map:
		keys := rv.MapKeys()
		keyStrings := make([]string, 0, len(keys))
		keyMap := make(map[string]reflect.Value, len(keys))
		for _, k := range keys {
			ks := fmt.Sprintf("%v", k.Interface())
			keyStrings = append(keyStrings, ks)
			keyMap[ks] = k
		}
		sort.Strings(keyStrings)
		parts := make([]string, 0, len(keyStrings))
		for _, ks := range keyStrings {
			val := rv.MapIndex(keyMap[ks]).Interface()
			parts = append(parts, ks+":"+stableAnySignature(val))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}
