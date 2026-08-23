package data

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	. "github.com/infrago/base"
)

const (
	valueKindArray   = "array"
	valueKindJSON    = "json"
	valueKindBinary  = "binary"
	valueKindUUID    = "uuid"
	valueKindDecimal = "decimal"
	valueKindTime    = "time"
)

func IsArrayVar(cfg Var) bool {
	return isArrayVar(cfg)
}

func IsJSONVar(cfg Var) bool {
	return valueKind(cfg) == valueKindJSON
}

func IsBinaryVar(cfg Var) bool {
	return valueKind(cfg) == valueKindBinary
}

func IsUUIDVar(cfg Var) bool {
	return valueKind(cfg) == valueKindUUID
}

func IsDecimalVar(cfg Var) bool {
	return valueKind(cfg) == valueKindDecimal
}

func IsTimeVar(cfg Var) bool {
	return valueKind(cfg) == valueKindTime
}

func BindJSONValue(value Any) (Any, bool) {
	if value == nil {
		return nil, true
	}
	switch value.(type) {
	case string, []byte:
		return value, true
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	return string(b), true
}

func BindBinaryValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return value, false
	}
}

func BindTextValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case []byte:
		return string(v), true
	case fmt.Stringer:
		return v.String(), true
	case string:
		return v, true
	default:
		return value, false
	}
}

func BindTimeValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case time.Time:
		return v, true
	case string:
		if t, ok := parseTimeValue(v); ok {
			return t, true
		}
	case []byte:
		if t, ok := parseTimeValue(string(v)); ok {
			return t, true
		}
	}
	return value, false
}

func DecodeJSONValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case []byte:
		return decodeJSONString(string(v))
	case string:
		return decodeJSONString(v)
	default:
		return value, false
	}
}

func DecodeBinaryValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return value, false
	}
}

func DecodeTextValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case []byte:
		return string(v), true
	case string:
		return v, true
	default:
		return value, false
	}
}

func DecodeTimeValue(value Any) (Any, bool) {
	switch v := value.(type) {
	case nil:
		return nil, true
	case time.Time:
		return v, true
	case []byte:
		if t, ok := parseTimeValue(string(v)); ok {
			return t, true
		}
	case string:
		if t, ok := parseTimeValue(v); ok {
			return t, true
		}
	case int64:
		return time.Unix(v, 0), true
	case int:
		return time.Unix(int64(v), 0), true
	case float64:
		return time.Unix(int64(v), 0), true
	}
	return value, false
}

func DecodePGArrayValue(cfg Var, value Any) (Any, bool) {
	text, ok := value.(string)
	if !ok {
		return nil, false
	}
	if !isArrayVar(cfg) {
		return nil, false
	}
	raw := strings.TrimSpace(text)
	if raw == "" {
		return value, false
	}
	items, ok := parsePGArrayLiteral(raw)
	if !ok {
		return nil, false
	}
	kind := strings.ToLower(strings.TrimSpace(cfg.Type))
	return normalizeParsedArrayValue(kind, items), true
}

func bindStructuredValue(d Dialect, cfg Var, value Any) (Any, bool) {
	// Collection fields are a portable JSON contract. PostgreSQL also exposes a
	// native array binder, but using it here makes the same `[string]` model write
	// `{a,b}` while MySQL and SQLite write `["a","b"]`. More importantly, the
	// portable migration maps collections to JSON/JSONB, where a PostgreSQL array
	// literal is invalid JSON. Encode collections before consulting the driver so
	// every backend persists the same representation.
	if isArrayVar(cfg) {
		return BindJSONValue(value)
	}
	if binder, ok := d.(ValueBinder); ok {
		if out, yes := binder.BindValue(cfg, value); yes {
			return out, true
		}
	}
	switch valueKind(cfg) {
	case valueKindJSON:
		return BindJSONValue(value)
	case valueKindBinary:
		return BindBinaryValue(value)
	case valueKindUUID, valueKindDecimal:
		return BindTextValue(value)
	case valueKindTime:
		return BindTimeValue(value)
	}
	return nil, false
}

func decodeStructuredValue(d Dialect, cfg Var, value Any) (Any, bool) {
	if decoder, ok := d.(ValueDecoder); ok {
		if out, yes := decoder.DecodeValue(cfg, value); yes {
			return out, true
		}
	}
	if isArrayVar(cfg) {
		if out, ok := decodeArrayValue(cfg, value); ok {
			return out, true
		}
	}
	switch valueKind(cfg) {
	case valueKindJSON:
		return DecodeJSONValue(value)
	case valueKindBinary:
		return DecodeBinaryValue(value)
	case valueKindUUID, valueKindDecimal:
		return DecodeTextValue(value)
	case valueKindTime:
		return DecodeTimeValue(value)
	default:
		return nil, false
	}
}

func decodeArrayValue(cfg Var, value Any) (Any, bool) {
	kind := strings.ToLower(strings.TrimSpace(cfg.Type))
	switch v := value.(type) {
	case string:
		raw := strings.TrimSpace(v)
		if raw == "" {
			return value, false
		}
		if items, ok := parsePGArrayLiteral(raw); ok {
			return normalizeParsedArrayValue(kind, items), true
		}
		if (strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]")) || (strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}")) {
			if parsed, ok := decodeJSONString(raw); ok {
				return normalizeDecodedArrayValue(kind, parsed), true
			}
		}
	case []byte:
		return decodeArrayValue(cfg, string(v))
	}
	return nil, false
}

func normalizeDecodedArrayValue(kind string, parsed Any) Any {
	if strings.Contains(kind, "uint") {
		if out, ok := toInt64Slice(parsed, true); ok {
			return out
		}
	}
	if strings.Contains(kind, "int") {
		if out, ok := toInt64Slice(parsed, false); ok {
			return out
		}
	}
	if strings.Contains(kind, "float") || strings.Contains(kind, "double") || strings.Contains(kind, "decimal") || strings.Contains(kind, "number") {
		if out, ok := toFloat64Slice(parsed); ok {
			return out
		}
	}
	return parsed
}

func decodeJSONString(raw string) (Any, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return raw, false
	}
	if !((strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]")) || (strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}"))) {
		return raw, false
	}
	var parsed Any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return raw, false
	}
	return parsed, true
}

func valueKind(cfg Var) string {
	kind := strings.ToLower(strings.TrimSpace(cfg.Type))
	kind = strings.TrimPrefix(kind, "[]")
	kind = strings.TrimPrefix(kind, "[")
	kind = strings.TrimSuffix(kind, "]")
	switch {
	case kind == "":
		return ""
	case isArrayVar(cfg):
		return valueKindArray
	case kind == "json" || kind == "jsonb" || strings.HasPrefix(kind, "map") || strings.HasPrefix(kind, "object"):
		return valueKindJSON
	case kind == "bytes" || kind == "byte" || kind == "binary" || kind == "blob" || kind == "bytea":
		return valueKindBinary
	case kind == "uuid":
		return valueKindUUID
	case kind == "decimal" || kind == "numeric" || kind == "number" || kind == "money":
		return valueKindDecimal
	case kind == "date" || kind == "time" || kind == "datetime" || kind == "timestamp" || kind == "timestamptz":
		return valueKindTime
	default:
		return ""
	}
}

func parseTimeValue(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Unix(n, 0), true
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, raw, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
