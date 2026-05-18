package data

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	. "github.com/infrago/base"
)

type writeNormalizeTestConn struct{}

type writeNormalizeTestDialect struct{}

type writeNormalizeBoundArray struct {
	value Any
}

type writeNormalizePlainDialect struct{}

func (c *writeNormalizeTestConn) Open() error    { return nil }
func (c *writeNormalizeTestConn) Close() error   { return nil }
func (c *writeNormalizeTestConn) Health() Health { return Health{} }
func (c *writeNormalizeTestConn) DB() *sql.DB    { return nil }
func (c *writeNormalizeTestConn) Dialect() Dialect {
	return writeNormalizeTestDialect{}
}

func (writeNormalizeTestDialect) Name() string            { return "pgsql" }
func (writeNormalizeTestDialect) Quote(s string) string   { return `"` + s + `"` }
func (writeNormalizeTestDialect) Placeholder(int) string  { return "$1" }
func (writeNormalizeTestDialect) SupportsILike() bool     { return true }
func (writeNormalizeTestDialect) SupportsReturning() bool { return true }
func (writeNormalizeTestDialect) BindArray(v any) any     { return writeNormalizeBoundArray{value: v} }

func (writeNormalizePlainDialect) Name() string            { return "pgsql" }
func (writeNormalizePlainDialect) Quote(s string) string   { return s }
func (writeNormalizePlainDialect) Placeholder(int) string  { return "$1" }
func (writeNormalizePlainDialect) SupportsILike() bool     { return true }
func (writeNormalizePlainDialect) SupportsReturning() bool { return true }

func TestNormalizeWriteValueForPostgresArrayField(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{Name: "normalize-array"},
				conn: &writeNormalizeTestConn{},
			},
			fields: Vars{
				"roleIds": Var{Type: "[int]"},
			},
		},
	}
	if got := table.normalizeWriteValue("roleIds", []int64{}); !reflect.DeepEqual(got, writeNormalizeBoundArray{value: []int64{}}) {
		t.Fatalf("expected bound postgres array wrapper, got %#v", got)
	}
	if got := table.normalizeWriteValue("roleIds", []int64{3, 5}); !reflect.DeepEqual(got, writeNormalizeBoundArray{value: []int64{3, 5}}) {
		t.Fatalf("expected bound postgres array wrapper, got %#v", got)
	}
}

func TestPostgresArrayLiteralEscapesStringValues(t *testing.T) {
	got := pgArrayLiteral([]string{`a,b`, `x"y`, `z\q`, "", "NULL", "plain"})
	want := `{"a,b","x\"y","z\\q","","NULL",plain}`
	if got != want {
		t.Fatalf("expected escaped postgres array literal %q, got %q", want, got)
	}
}

func TestBindArrayValueFallsBackToLiteral(t *testing.T) {
	got := bindArrayValue(writeNormalizePlainDialect{}, []string{"a,b"})
	if got != `{"a,b"}` {
		t.Fatalf("expected literal fallback, got %#v", got)
	}
}

func TestDecodeStructuredFieldValueForPostgresArrayLiteral(t *testing.T) {
	got := decodeStructuredFieldValue(writeNormalizeTestDialect{}, Var{Type: "[string]"}, `{"a,b","x\"y","z\\q","","NULL",plain}`)
	items, ok := got.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T %#v", got, got)
	}
	expect := []string{"a,b", `x"y`, `z\q`, "", "NULL", "plain"}
	if len(items) != len(expect) {
		t.Fatalf("expected %d items, got %d: %#v", len(expect), len(items), items)
	}
	for i := range expect {
		if items[i] != expect[i] {
			t.Fatalf("item %d expected %#v, got %#v", i, expect[i], items[i])
		}
	}
}

func TestDecodeStructuredFieldValueForPostgresIntArrayLiteral(t *testing.T) {
	got := decodeStructuredFieldValue(writeNormalizeTestDialect{}, Var{Type: "[int]"}, `{1,2,3}`)
	items, ok := got.([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T %#v", got, got)
	}
	if len(items) != 3 || items[0] != 1 || items[1] != 2 || items[2] != 3 {
		t.Fatalf("unexpected int array decode: %#v", items)
	}
}

func TestDecodeStructuredFieldValueForPostgresMultiDimIntArrayLiteral(t *testing.T) {
	got := decodeStructuredFieldValue(writeNormalizeTestDialect{}, Var{Type: "[int]"}, `{{1,2},{3,4}}`)
	items, ok := got.([][]int64)
	if !ok {
		t.Fatalf("expected [][]int64, got %T %#v", got, got)
	}
	if len(items) != 2 || len(items[0]) != 2 || len(items[1]) != 2 {
		t.Fatalf("unexpected multidim shape: %#v", items)
	}
	if items[0][0] != 1 || items[0][1] != 2 || items[1][0] != 3 || items[1][1] != 4 {
		t.Fatalf("unexpected multidim values: %#v", items)
	}
}

func TestDecodeStructuredFieldValueForArrayFlag(t *testing.T) {
	got := decodeStructuredFieldValue(writeNormalizeTestDialect{}, Var{Type: "string", Setting: Map{"array": true}}, `{alpha,beta}`)
	items, ok := got.([]string)
	if !ok {
		t.Fatalf("expected []string for array flag, got %T %#v", got, got)
	}
	if len(items) != 2 || items[0] != "alpha" || items[1] != "beta" {
		t.Fatalf("unexpected array flag decode: %#v", items)
	}
}

func TestNormalizeWriteValueForPostgresArrayFlag(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{Name: "normalize-array-flag"},
				conn: &writeNormalizeTestConn{},
			},
			fields: Vars{
				"tags": Var{
					Type:    "string",
					Setting: Map{"array": true},
				},
			},
		},
	}

	if got := table.normalizeWriteValue("tags", []string{}); !reflect.DeepEqual(got, writeNormalizeBoundArray{value: []string{}}) {
		t.Fatalf("expected empty postgres array literal by flag, got %#v", got)
	}
}

func TestBindStructuredValueForCommonTypes(t *testing.T) {
	jsonValue, ok := bindStructuredValue(writeNormalizePlainDialect{}, Var{Type: "json"}, Map{"name": "alice"})
	if !ok || jsonValue != `{"name":"alice"}` {
		t.Fatalf("expected encoded json value, got %#v ok=%v", jsonValue, ok)
	}

	binaryValue, ok := bindStructuredValue(writeNormalizePlainDialect{}, Var{Type: "bytea"}, "hello")
	if !ok || !reflect.DeepEqual(binaryValue, []byte("hello")) {
		t.Fatalf("expected binary value, got %#v ok=%v", binaryValue, ok)
	}

	uuidValue, ok := bindStructuredValue(writeNormalizePlainDialect{}, Var{Type: "uuid"}, []byte("9b4f9a87-6ad6-4be4-9f51-0dfb4747d8a1"))
	if !ok || uuidValue != "9b4f9a87-6ad6-4be4-9f51-0dfb4747d8a1" {
		t.Fatalf("expected uuid text value, got %#v ok=%v", uuidValue, ok)
	}

	timeValue, ok := bindStructuredValue(writeNormalizePlainDialect{}, Var{Type: "datetime"}, "2026-05-15 10:20:30")
	if !ok {
		t.Fatalf("expected parsed time value")
	}
	if _, yes := timeValue.(time.Time); !yes {
		t.Fatalf("expected time.Time, got %T %#v", timeValue, timeValue)
	}
}

func TestDecodeStructuredFieldValueForCommonTypes(t *testing.T) {
	jsonValue := decodeStructuredFieldValue(writeNormalizePlainDialect{}, Var{Type: "json"}, `{"name":"alice","age":3}`)
	jsonMap, ok := jsonValue.(map[string]Any)
	if !ok {
		t.Fatalf("expected json map, got %T %#v", jsonValue, jsonValue)
	}
	if jsonMap["name"] != "alice" {
		t.Fatalf("unexpected json decode: %#v", jsonMap)
	}

	binaryValue := decodeStructuredFieldValue(writeNormalizePlainDialect{}, Var{Type: "blob"}, "hello")
	if !reflect.DeepEqual(binaryValue, []byte("hello")) {
		t.Fatalf("expected binary bytes, got %#v", binaryValue)
	}

	decimalValue := decodeStructuredFieldValue(writeNormalizePlainDialect{}, Var{Type: "numeric"}, []byte("123.45"))
	if decimalValue != "123.45" {
		t.Fatalf("expected decimal text, got %#v", decimalValue)
	}

	timeValue := decodeStructuredFieldValue(writeNormalizePlainDialect{}, Var{Type: "timestamp"}, "2026-05-15T10:20:30Z")
	if _, ok := timeValue.(time.Time); !ok {
		t.Fatalf("expected decoded time.Time, got %T %#v", timeValue, timeValue)
	}
}
