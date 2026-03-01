package data

import (
	"encoding/json"
	"fmt"
	"strings"

	. "github.com/infrago/base"
)

type SQLBuilder struct {
	dialect Dialect
	args    []Any
	index   int
	// Optional resolvers for `a.b` ambiguity:
	// alias.field vs jsonPath.
	isAlias     func(string) bool
	isJSONField func(string) bool
	toStorage   func(string) string
}

func NewSQLBuilder(d Dialect) *SQLBuilder {
	return &SQLBuilder{dialect: d, args: make([]Any, 0), index: 1}
}

func (b *SQLBuilder) Args() []Any { return b.args }

func (b *SQLBuilder) quoteField(field string) string {
	parts := strings.Split(field, ".")
	for i, p := range parts {
		parts[i] = b.dialect.Quote(strings.TrimSpace(p))
	}
	return strings.Join(parts, ".")
}

func (b *SQLBuilder) bind(v Any) string {
	ph := b.dialect.Placeholder(b.index)
	b.index++
	b.args = append(b.args, v)
	return ph
}

func (b *SQLBuilder) CompileExpr(e Expr) (string, error) {
	switch vv := e.(type) {
	case nil:
		return "1=1", nil
	case TrueExpr:
		return "1=1", nil
	case AndExpr:
		parts := make([]string, 0, len(vv.Items))
		for _, item := range vv.Items {
			sql, err := b.CompileExpr(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+sql+")")
		}
		if len(parts) == 0 {
			return "1=1", nil
		}
		return strings.Join(parts, " AND "), nil
	case OrExpr:
		parts := make([]string, 0, len(vv.Items))
		for _, item := range vv.Items {
			sql, err := b.CompileExpr(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, "("+sql+")")
		}
		if len(parts) == 0 {
			return "1=1", nil
		}
		return strings.Join(parts, " OR "), nil
	case NotExpr:
		sql, err := b.CompileExpr(vv.Item)
		if err != nil {
			return "", err
		}
		return "NOT (" + sql + ")", nil
	case ExistsExpr:
		field := b.quoteField(vv.Field)
		if vv.Yes {
			return field + " IS NOT NULL", nil
		}
		return field + " IS NULL", nil
	case NullExpr:
		field := b.quoteField(vv.Field)
		if vv.Yes {
			return field + " IS NULL", nil
		}
		return field + " IS NOT NULL", nil
	case RawExpr:
		for _, p := range vv.Params {
			_ = b.bind(p)
		}
		return vv.SQL, nil
	case CmpExpr:
		return b.compileCmp(vv)
	default:
		return "", fmt.Errorf("unsupported expression %T", vv)
	}
}

func (b *SQLBuilder) compileCmp(c CmpExpr) (string, error) {
	field, err := b.compileFieldExpr(c.Field)
	if err != nil {
		return "", err
	}
	if ref, ok := c.Value.(FieldRef); ok {
		rf, err := b.compileFieldExpr(string(ref))
		if err != nil {
			return "", err
		}
		switch c.Op {
		case OpEq:
			return field + " = " + rf, nil
		case OpNe:
			return field + " <> " + rf, nil
		case OpGt:
			return field + " > " + rf, nil
		case OpGte:
			return field + " >= " + rf, nil
		case OpLt:
			return field + " < " + rf, nil
		case OpLte:
			return field + " <= " + rf, nil
		default:
			return "", fmt.Errorf("operator %s does not support field ref", c.Op)
		}
	}
	switch c.Op {
	case OpEq:
		if c.Value == nil {
			return field + " IS NULL", nil
		}
		return field + " = " + b.bind(c.Value), nil
	case OpNe:
		if c.Value == nil {
			return field + " IS NOT NULL", nil
		}
		return field + " <> " + b.bind(c.Value), nil
	case OpGt:
		return field + " > " + b.bind(c.Value), nil
	case OpGte:
		return field + " >= " + b.bind(c.Value), nil
	case OpLt:
		return field + " < " + b.bind(c.Value), nil
	case OpLte:
		return field + " <= " + b.bind(c.Value), nil
	case OpIn:
		vals := flattenSlice(c.Value)
		if len(vals) == 0 {
			return "1=2", nil
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			parts = append(parts, b.bind(v))
		}
		return fmt.Sprintf("%s IN (%s)", field, strings.Join(parts, ",")), nil
	case OpNin:
		vals := flattenSlice(c.Value)
		if len(vals) == 0 {
			return "1=1", nil
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			parts = append(parts, b.bind(v))
		}
		return fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(parts, ",")), nil
	case OpLike:
		return field + " LIKE " + b.bind(c.Value), nil
	case OpILike:
		if b.dialect.SupportsILike() {
			return field + " ILIKE " + b.bind(c.Value), nil
		}
		return "LOWER(" + field + ") LIKE LOWER(" + b.bind(c.Value) + ")", nil
	case OpRegex:
		// portable fallback for SQL dialects without regex operator support.
		val := fmt.Sprintf("%v", c.Value)
		val = strings.TrimPrefix(val, "^")
		val = strings.TrimSuffix(val, "$")
		val = strings.ReplaceAll(val, ".*", "%")
		if !strings.Contains(val, "%") {
			val = "%" + val + "%"
		}
		if b.dialect.SupportsILike() {
			return field + " ILIKE " + b.bind(val), nil
		}
		return "LOWER(" + field + ") LIKE LOWER(" + b.bind(val) + ")", nil
	case OpContains:
		return b.compileContains(field, c.Value)
	case OpOverlap:
		return b.compileOverlap(field, c.Value)
	case OpElemMatch:
		return b.compileElemMatch(field, c.Value)
	default:
		return "", fmt.Errorf("unsupported compare operator %s", c.Op)
	}
}

func (b *SQLBuilder) compileFieldExpr(field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return "", fmt.Errorf("empty field")
	}
	if !strings.Contains(field, ".") {
		return b.quoteField(field), nil
	}
	parts := strings.Split(field, ".")
	head := strings.TrimSpace(parts[0])
	tail := parts[1:]
	if b.isAlias != nil && b.isAlias(head) {
		return b.quoteField(field), nil
	}
	if b.isJSONField != nil && b.isJSONField(head) {
		return b.compileJSONPathExpr(head, tail)
	}
	return "", fmt.Errorf("ambiguous field %s: neither alias.field nor jsonPath on JSON field", field)
}

func (b *SQLBuilder) compileJSONPathExpr(head string, path []string) (string, error) {
	if b.toStorage != nil {
		head = b.toStorage(head)
	}
	base := b.quoteField(head)
	name := strings.ToLower(strings.TrimSpace(b.dialect.Name()))
	if len(path) == 0 {
		return base, nil
	}
	switch {
	case name == "pgsql" || name == "postgres":
		if len(path) == 1 {
			return fmt.Sprintf("(%s->>'%s')", base, path[0]), nil
		}
		return fmt.Sprintf("(%s#>>'{%s}')", base, strings.Join(path, ",")), nil
	case name == "mysql":
		return fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(%s, '$.%s'))", base, strings.Join(path, ".")), nil
	case name == "sqlite":
		return fmt.Sprintf("json_extract(%s, '$.%s')", base, strings.Join(path, ".")), nil
	default:
		return "", fmt.Errorf("dialect %s does not support json path field %s.%s", name, head, strings.Join(path, "."))
	}
}

func (b *SQLBuilder) compileContains(field string, value Any) (string, error) {
	name := strings.ToLower(b.dialect.Name())
	switch name {
	case "pgsql", "postgres":
		raw, err := jsonString(value)
		if err != nil {
			return "", err
		}
		return field + " @> " + b.bind(raw), nil
	case "mysql":
		raw, err := jsonString(value)
		if err != nil {
			return "", err
		}
		return "JSON_CONTAINS(" + field + ", CAST(" + b.bind(raw) + " AS JSON))", nil
	case "sqlite":
		raw, err := jsonString(value)
		if err != nil {
			return "", err
		}
		return "instr(" + field + ", " + b.bind(raw) + ") > 0", nil
	default:
		return "", fmt.Errorf("dialect %s does not support $contains", b.dialect.Name())
	}
}

func (b *SQLBuilder) compileOverlap(field string, value Any) (string, error) {
	name := strings.ToLower(b.dialect.Name())
	switch name {
	case "pgsql", "postgres":
		// overlap for arrays
		return field + " && " + b.bind(pgArrayLiteral(value)), nil
	case "mysql":
		raw, err := jsonString(value)
		if err != nil {
			return "", err
		}
		return "JSON_OVERLAPS(" + field + ", CAST(" + b.bind(raw) + " AS JSON))", nil
	case "sqlite":
		return "", fmt.Errorf("sqlite does not support $overlap directly")
	default:
		return "", fmt.Errorf("dialect %s does not support $overlap", b.dialect.Name())
	}
}

func (b *SQLBuilder) compileElemMatch(field string, value Any) (string, error) {
	name := strings.ToLower(b.dialect.Name())
	switch name {
	case "pgsql", "postgres":
		arr := []Any{value}
		raw, err := jsonString(arr)
		if err != nil {
			return "", err
		}
		return field + " @> " + b.bind(raw), nil
	case "mysql":
		arr := []Any{value}
		raw, err := jsonString(arr)
		if err != nil {
			return "", err
		}
		return "JSON_CONTAINS(" + field + ", CAST(" + b.bind(raw) + " AS JSON))", nil
	case "sqlite":
		return "", fmt.Errorf("sqlite does not support $elemMatch directly")
	default:
		return "", fmt.Errorf("dialect %s does not support $elemMatch", b.dialect.Name())
	}
}

func (b *SQLBuilder) CompileWhere(q Query) (string, []Any, error) {
	if strings.TrimSpace(q.RawWhere) != "" {
		params := make([]Any, 0, len(b.args)+len(q.RawParams))
		params = append(params, b.args...)
		params = append(params, q.RawParams...)
		return q.RawWhere, params, nil
	}
	sql, err := b.CompileExpr(q.Filter)
	if err != nil {
		return "", nil, err
	}
	return sql, b.args, nil
}

func BuildOrderBy(q Query, dialect Dialect) string {
	if len(q.Sort) == 0 {
		return ""
	}
	parts := make([]string, 0, len(q.Sort))
	for _, s := range q.Sort {
		field := quoteField(dialect, s.Field)
		if s.Desc {
			parts = append(parts, field+" DESC")
		} else {
			parts = append(parts, field+" ASC")
		}
	}
	return " ORDER BY " + strings.Join(parts, ",")
}

func BuildGroupBy(fields []string, dialect Dialect) string {
	if len(fields) == 0 {
		return ""
	}
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		parts = append(parts, quoteField(dialect, field))
	}
	return " GROUP BY " + strings.Join(parts, ",")
}

func BuildLimitOffset(q Query, startIndex int, dialect Dialect, args *[]Any) string {
	out := ""
	if q.Limit > 0 {
		out += " LIMIT " + dialect.Placeholder(startIndex)
		*args = append(*args, q.Limit)
		startIndex++
	}
	if q.Offset > 0 {
		out += " OFFSET " + dialect.Placeholder(startIndex)
		*args = append(*args, q.Offset)
	}
	return out
}

func quoteField(d Dialect, field string) string {
	parts := strings.Split(field, ".")
	for i, p := range parts {
		parts[i] = d.Quote(strings.TrimSpace(p))
	}
	return strings.Join(parts, ".")
}

func flattenSlice(v Any) []Any {
	switch vv := v.(type) {
	case []Any:
		return vv
	case []string:
		out := make([]Any, 0, len(vv))
		for _, v := range vv {
			out = append(out, v)
		}
		return out
	case []int:
		out := make([]Any, 0, len(vv))
		for _, v := range vv {
			out = append(out, v)
		}
		return out
	case []int64:
		out := make([]Any, 0, len(vv))
		for _, v := range vv {
			out = append(out, v)
		}
		return out
	case []float64:
		out := make([]Any, 0, len(vv))
		for _, v := range vv {
			out = append(out, v)
		}
		return out
	default:
		return []Any{v}
	}
}

func jsonString(v Any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func pgArrayLiteral(v Any) string {
	vals := flattenSlice(v)
	if len(vals) == 0 {
		return "{}"
	}
	items := make([]string, 0, len(vals))
	for _, item := range vals {
		items = append(items, fmt.Sprintf("%v", item))
	}
	return "{" + strings.Join(items, ",") + "}"
}
