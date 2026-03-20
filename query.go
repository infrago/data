package data

import (
	"fmt"
	"strconv"
	"strings"

	. "github.com/infrago/base"
)

type (
	FieldRef string

	Query struct {
		Select      []string
		Filter      Expr
		Sort        []Sort
		Limit       int64
		Offset      int64
		After       Map
		WithCount   bool
		WithDeleted bool
		OnlyDeleted bool
		Unscoped    bool
		Unsafe      bool
		Batch       int64
		Group       []string
		Aggs        []Agg
		Having      Expr
		Joins       []Join

		RawWhere  string
		RawParams []Any
	}

	Join struct {
		From         string
		Alias        string
		Type         string
		LocalField   string
		ForeignField string
		On           Expr
	}

	Sort struct {
		Field string
		Desc  bool
	}

	Agg struct {
		Alias string
		Op    string
		Field string
	}

	Expr interface{ isExpr() }

	AndExpr struct{ Items []Expr }
	OrExpr  struct{ Items []Expr }
	NotExpr struct{ Item Expr }
	CmpExpr struct {
		Field string
		Op    string
		Value Any
	}
	ExistsExpr struct {
		Field string
		Yes   bool
	}
	NullExpr struct {
		Field string
		Yes   bool
	}
	RawExpr struct {
		SQL    string
		Params []Any
	}
	TrueExpr struct{}
)

func (AndExpr) isExpr()    {}
func (OrExpr) isExpr()     {}
func (NotExpr) isExpr()    {}
func (CmpExpr) isExpr()    {}
func (ExistsExpr) isExpr() {}
func (NullExpr) isExpr()   {}
func (RawExpr) isExpr()    {}
func (TrueExpr) isExpr()   {}

func ParseQuery(args ...Any) (Query, error) {
	q := Query{Filter: TrueExpr{}, WithCount: false}
	if len(args) == 0 {
		return q, nil
	}
	cacheKey := queryCompileKey(args)
	if cacheKey != "" {
		if cached, ok := loadQueryCompile(cacheKey); ok {
			return cached, nil
		}
	}

	if raw, ok := args[0].(string); ok && strings.TrimSpace(raw) != "" {
		q.RawWhere = raw
		if len(args) > 1 {
			q.RawParams = make([]Any, 0, len(args)-1)
			for _, arg := range args[1:] {
				q.RawParams = append(q.RawParams, arg)
			}
		}
		if cacheKey != "" {
			storeQueryCompile(cacheKey, q)
		}
		return q, nil
	}

	maps := make([]Map, 0)
	for _, arg := range args {
		if m, ok := arg.(Map); ok {
			maps = append(maps, m)
		}
	}
	if len(maps) == 0 {
		return q, nil
	}

	orRoots := make([]Expr, 0, len(maps))
	for _, m := range maps {
		e, opts, err := parseFilterMap(m)
		if err != nil {
			return Query{}, err
		}
		if e != nil {
			orRoots = append(orRoots, e)
		}
		mergeOptions(&q, opts)
	}

	switch len(orRoots) {
	case 0:
		q.Filter = TrueExpr{}
	case 1:
		q.Filter = orRoots[0]
	default:
		q.Filter = OrExpr{Items: orRoots}
	}
	if cacheKey != "" {
		storeQueryCompile(cacheKey, q)
	}

	return q, nil
}

func Ref(field string) FieldRef { return FieldRef(strings.TrimSpace(field)) }

type queryOptions struct {
	selects     []string
	sorts       []Sort
	limit       *int64
	offset      *int64
	after       Map
	withCount   *bool
	withDeleted *bool
	onlyDeleted *bool
	unscoped    *bool
	unsafe      *bool
	batch       *int64
	group       []string
	aggs        []Agg
	having      Expr
	joins       []Join
}

func mergeOptions(q *Query, opts queryOptions) {
	if len(opts.selects) > 0 {
		q.Select = opts.selects
	}
	if len(opts.sorts) > 0 {
		q.Sort = opts.sorts
	}
	if opts.limit != nil {
		q.Limit = *opts.limit
	}
	if opts.offset != nil {
		q.Offset = *opts.offset
	}
	if len(opts.after) > 0 {
		q.After = opts.after
	}
	if opts.withCount != nil {
		q.WithCount = *opts.withCount
	}
	if opts.withDeleted != nil {
		q.WithDeleted = *opts.withDeleted
	}
	if opts.onlyDeleted != nil {
		q.OnlyDeleted = *opts.onlyDeleted
	}
	if opts.unscoped != nil {
		q.Unscoped = *opts.unscoped
	}
	if opts.unsafe != nil {
		q.Unsafe = *opts.unsafe
	}
	if opts.batch != nil {
		q.Batch = *opts.batch
	}
	if len(opts.group) > 0 {
		q.Group = opts.group
	}
	if len(opts.aggs) > 0 {
		q.Aggs = opts.aggs
	}
	if opts.having != nil {
		q.Having = opts.having
	}
	if len(opts.joins) > 0 {
		q.Joins = opts.joins
	}
}

func mergeQueryOptions(dst *queryOptions, src queryOptions) {
	if dst == nil {
		return
	}
	if len(src.selects) > 0 {
		dst.selects = src.selects
	}
	if len(src.sorts) > 0 {
		dst.sorts = src.sorts
	}
	if src.limit != nil {
		dst.limit = src.limit
	}
	if src.offset != nil {
		dst.offset = src.offset
	}
	if len(src.after) > 0 {
		dst.after = src.after
	}
	if src.withCount != nil {
		dst.withCount = src.withCount
	}
	if src.withDeleted != nil {
		dst.withDeleted = src.withDeleted
	}
	if src.onlyDeleted != nil {
		dst.onlyDeleted = src.onlyDeleted
	}
	if src.unscoped != nil {
		dst.unscoped = src.unscoped
	}
	if src.unsafe != nil {
		dst.unsafe = src.unsafe
	}
	if src.batch != nil {
		dst.batch = src.batch
	}
	if len(src.group) > 0 {
		dst.group = src.group
	}
	if len(src.aggs) > 0 {
		dst.aggs = src.aggs
	}
	if src.having != nil {
		dst.having = src.having
	}
	if len(src.joins) > 0 {
		dst.joins = src.joins
	}
}

func combineAndExprs(items []Expr) Expr {
	switch len(items) {
	case 0:
		return nil
	case 1:
		return items[0]
	default:
		return AndExpr{Items: items}
	}
}

func parseNestedFilterValue(val Any) (Expr, queryOptions, error) {
	opts := queryOptions{}
	items := make([]Expr, 0)
	switch vv := val.(type) {
	case Map:
		e, nested, err := parseFilterMap(vv)
		if err != nil {
			return nil, opts, err
		}
		mergeQueryOptions(&opts, nested)
		if e != nil {
			items = append(items, e)
		}
	case []Map:
		for _, item := range vv {
			e, nested, err := parseFilterMap(item)
			if err != nil {
				return nil, opts, err
			}
			mergeQueryOptions(&opts, nested)
			if e != nil {
				items = append(items, e)
			}
		}
	case []Any:
		for _, item := range vv {
			m, ok := item.(Map)
			if !ok {
				continue
			}
			e, nested, err := parseFilterMap(m)
			if err != nil {
				return nil, opts, err
			}
			mergeQueryOptions(&opts, nested)
			if e != nil {
				items = append(items, e)
			}
		}
	}
	return combineAndExprs(items), opts, nil
}

func parseFilterMap(m Map) (Expr, queryOptions, error) {
	opts := queryOptions{}
	ands := make([]Expr, 0)

	for key, val := range m {
		if strings.HasPrefix(key, "$") {
			switch key {
			case OptSelect:
				opts.selects = parseStringList(val)
			case OptSort:
				opts.sorts = parseSorts(val)
			case OptLimit:
				if vv, ok := parseInt64(val); ok {
					opts.limit = &vv
				}
			case OptOffset:
				if vv, ok := parseInt64(val); ok {
					opts.offset = &vv
				}
			case OptAfter:
				opts.after = parseAfter(val)
			case OptWithCount:
				if vv, ok := parseBool(val); ok {
					opts.withCount = &vv
				}
			case OptWithDeleted, "$with_deleted":
				if vv, ok := parseBool(val); ok {
					opts.withDeleted = &vv
				}
			case OptOnlyDeleted, "$only_deleted":
				if vv, ok := parseBool(val); ok {
					opts.onlyDeleted = &vv
				}
			case OptUnscoped:
				if vv, ok := parseBool(val); ok {
					opts.unscoped = &vv
				}
			case OptUnsafe:
				if vv, ok := parseBool(val); ok {
					opts.unsafe = &vv
				}
			case OptBatch:
				if vv, ok := parseInt64(val); ok {
					opts.batch = &vv
				}
			case OptGroup:
				opts.group = parseStringList(val)
			case OptAgg:
				opts.aggs = parseAggs(val)
			case OptHaving:
				if hm, ok := val.(Map); ok {
					he, _, err := parseFilterMap(hm)
					if err != nil {
						return nil, opts, err
					}
					opts.having = he
				}
			case OptFilter, OptFilters:
				e, nested, err := parseNestedFilterValue(val)
				if err != nil {
					return nil, opts, err
				}
				mergeQueryOptions(&opts, nested)
				if e != nil {
					ands = append(ands, e)
				}
			case OptJoin:
				opts.joins = parseJoins(val)
			case OpAnd:
				e, err := parseLogicItems(OpAnd, val)
				if err != nil {
					return nil, opts, err
				}
				if e != nil {
					ands = append(ands, e)
				}
			case OpOr:
				e, err := parseLogicItems(OpOr, val)
				if err != nil {
					return nil, opts, err
				}
				if e != nil {
					ands = append(ands, e)
				}
			case OpNor:
				e, err := parseLogicItems(OpOr, val)
				if err != nil {
					return nil, opts, err
				}
				if e != nil {
					ands = append(ands, NotExpr{Item: e})
				}
			case OpNot:
				if nm, ok := val.(Map); ok {
					e, _, err := parseFilterMap(nm)
					if err != nil {
						return nil, opts, err
					}
					if e != nil {
						ands = append(ands, NotExpr{Item: e})
					}
				}
			}
			continue
		}

		exprs, err := parseFieldExpr(key, val)
		if err != nil {
			return nil, opts, err
		}
		ands = append(ands, exprs...)
	}

	switch len(ands) {
	case 0:
		return TrueExpr{}, opts, nil
	case 1:
		return ands[0], opts, nil
	default:
		return AndExpr{Items: ands}, opts, nil
	}
}

func parseLogicItems(op string, val Any) (Expr, error) {
	items := make([]Expr, 0)
	switch vv := val.(type) {
	case []Map:
		for _, m := range vv {
			e, _, err := parseFilterMap(m)
			if err != nil {
				return nil, err
			}
			items = append(items, e)
		}
	case []Any:
		for _, item := range vv {
			if m, ok := item.(Map); ok {
				e, _, err := parseFilterMap(m)
				if err != nil {
					return nil, err
				}
				items = append(items, e)
			}
		}
	case Map:
		e, _, err := parseFilterMap(vv)
		if err != nil {
			return nil, err
		}
		items = append(items, e)
	}
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) == 1 {
		return items[0], nil
	}
	if op == OpAnd {
		return AndExpr{Items: items}, nil
	}
	return OrExpr{Items: items}, nil
}

func parseFieldExpr(field string, val Any) ([]Expr, error) {
	if val == nil {
		return []Expr{NullExpr{Field: field, Yes: true}}, nil
	}
	if opMap, ok := val.(Map); ok {
		exprs := make([]Expr, 0, len(opMap))
		for op, vv := range opMap {
			if sv, ok := vv.(string); ok && strings.HasPrefix(strings.TrimSpace(sv), "$field:") {
				vv = FieldRef(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(sv), "$field:")))
			}
			switch op {
			case OpEq, OpNe, OpGt, OpGte, OpLt, OpLte, OpIn, OpNin, OpLike, OpILike, OpRegex, OpContains, OpOverlap, OpElemMatch:
				exprs = append(exprs, CmpExpr{Field: field, Op: op, Value: vv})
			case OpExists:
				yes, _ := parseBool(vv)
				exprs = append(exprs, ExistsExpr{Field: field, Yes: yes})
			case OpNull:
				yes, _ := parseBool(vv)
				exprs = append(exprs, NullExpr{Field: field, Yes: yes})
			case OpNot:
				nested, err := parseFieldExpr(field, vv)
				if err != nil {
					return nil, err
				}
				if len(nested) == 1 {
					exprs = append(exprs, NotExpr{Item: nested[0]})
				} else {
					exprs = append(exprs, NotExpr{Item: AndExpr{Items: nested}})
				}
			default:
				return nil, fmt.Errorf("unsupported operator %s", op)
			}
		}
		return exprs, nil
	}

	switch vv := val.(type) {
	case []Any, []string, []int, []int64, []float64:
		return []Expr{CmpExpr{Field: field, Op: OpIn, Value: vv}}, nil
	default:
		return []Expr{CmpExpr{Field: field, Op: OpEq, Value: vv}}, nil
	}
}

func parseSorts(val Any) []Sort {
	out := make([]Sort, 0)
	switch vv := val.(type) {
	case Map:
		for k, v := range vv {
			s := Sort{Field: k}
			s.Desc = parseSortDesc(v)
			out = append(out, s)
		}
	case []Map:
		for _, m := range vv {
			out = append(out, parseSorts(m)...)
		}
	case []Any:
		for _, item := range vv {
			out = append(out, parseSorts(item)...)
		}
	case string:
		for _, part := range strings.Split(vv, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if strings.HasPrefix(part, "-") {
				out = append(out, Sort{Field: strings.TrimPrefix(part, "-"), Desc: true})
			} else {
				out = append(out, Sort{Field: strings.TrimPrefix(part, "+"), Desc: false})
			}
		}
	}
	return out
}

func parseSortDesc(v Any) bool {
	switch vv := v.(type) {
	case string:
		l := strings.ToLower(strings.TrimSpace(vv))
		return l == "desc" || l == "-1" || l == "-"
	case int:
		return vv < 0
	case int64:
		return vv < 0
	case float64:
		return vv < 0
	case bool:
		return vv
	default:
		return false
	}
}

func parseStringList(val Any) []string {
	out := make([]string, 0)
	switch vv := val.(type) {
	case string:
		for _, part := range strings.Split(vv, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	case []string:
		for _, part := range vv {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	case []Any:
		for _, item := range vv {
			if s, ok := item.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

func parseJoins(val Any) []Join {
	items := make([]Map, 0)
	switch vv := val.(type) {
	case Map:
		items = append(items, vv)
	case []Map:
		items = append(items, vv...)
	case []Any:
		for _, item := range vv {
			if m, ok := item.(Map); ok {
				items = append(items, m)
			}
		}
	}

	out := make([]Join, 0, len(items))
	for _, item := range items {
		j := Join{Type: "left"}
		if v, ok := item["from"].(string); ok {
			j.From = v
		}
		if v, ok := item["alias"].(string); ok {
			j.Alias = v
		}
		if v, ok := item["type"].(string); ok && v != "" {
			j.Type = strings.ToLower(v)
		}
		if v, ok := item["localField"].(string); ok {
			j.LocalField = v
		}
		if v, ok := item["foreignField"].(string); ok {
			j.ForeignField = v
		}
		if on, ok := item["on"].(Map); ok {
			e, _, err := parseFilterMap(on)
			if err == nil {
				j.On = e
			}
		} else if ons, ok := item["on"].([]Map); ok {
			e, err := parseLogicItems(OpAnd, ons)
			if err == nil {
				j.On = e
			}
		} else if ons, ok := item["on"].([]Any); ok {
			e, err := parseLogicItems(OpAnd, ons)
			if err == nil {
				j.On = e
			}
		}
		if j.From != "" {
			out = append(out, j)
		}
	}
	return out
}

func parseAfter(val Any) Map {
	out := Map{}
	switch vv := val.(type) {
	case Map:
		for k, v := range vv {
			out[k] = v
		}
	case string:
		out["$value"] = vv
	case int, int64, float64:
		out["$value"] = vv
	}
	return out
}

func parseAggs(val Any) []Agg {
	out := make([]Agg, 0)
	switch vv := val.(type) {
	case Map:
		for alias, raw := range vv {
			switch r := raw.(type) {
			case string:
				op := strings.ToLower(strings.TrimSpace(r))
				if op != "" {
					out = append(out, Agg{Alias: alias, Op: op, Field: "*"})
				}
			case Map:
				for op, field := range r {
					agg := Agg{Alias: alias, Op: strings.ToLower(strings.TrimSpace(op))}
					if s, ok := field.(string); ok && strings.TrimSpace(s) != "" {
						agg.Field = strings.TrimSpace(s)
					} else {
						agg.Field = "*"
					}
					if agg.Op != "" {
						out = append(out, agg)
					}
				}
			}
		}
	case []Map:
		for _, m := range vv {
			out = append(out, parseAggs(m)...)
		}
	case []Any:
		for _, item := range vv {
			out = append(out, parseAggs(item)...)
		}
	}
	return out
}

func parseInt64(v Any) (int64, bool) {
	switch vv := v.(type) {
	case int:
		return int64(vv), true
	case int64:
		return vv, true
	case float64:
		return int64(vv), true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(vv), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func parseBool(v Any) (bool, bool) {
	switch vv := v.(type) {
	case bool:
		return vv, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(vv))
		if err != nil {
			return false, false
		}
		return b, true
	case int:
		return vv != 0, true
	case int64:
		return vv != 0, true
	default:
		return false, false
	}
}
