package data

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrNotFound        = errors.New("data: not found")
	ErrConflict        = errors.New("data: conflict")
	ErrDuplicate       = errors.New("data: duplicate")
	ErrForeignKey      = errors.New("data: foreign key")
	ErrUnsupported     = errors.New("data: unsupported")
	ErrInvalidQuery    = errors.New("data: invalid query")
	ErrInvalidUpdate   = errors.New("data: invalid update")
	ErrInvalidSequence = errors.New("data: invalid sequence")
	ErrTxFailed        = errors.New("data: tx failed")
	ErrValidation      = errors.New("data: validation")
	ErrDriver          = errors.New("data: driver")
	ErrTimeout         = errors.New("data: timeout")
	ErrCanceled        = errors.New("data: canceled")
)

type DataError struct {
	Op   string
	Code error
	Err  error
	Kind string
}

func (e *DataError) Error() string {
	if e == nil {
		return ""
	}
	if e.Op == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *DataError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.Err != nil {
		return e.Err
	}
	return e.Code
}

func (e *DataError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == ErrConflict && (e.Code == ErrDuplicate || e.Code == ErrForeignKey) {
		return true
	}
	return target == e.Code
}

func wrapErr(op string, code error, err error) error {
	if err == nil {
		return nil
	}
	code = refineErrorCode(code, err)
	return &DataError{Op: op, Code: code, Err: err, Kind: classifyErrorKind(code, err)}
}

func Error(op string, code error, err error) error {
	return wrapErr(op, code, err)
}

func classifySQLError(err error) error {
	return classifySQLErrorWithDialect(nil, err)
}

func classifySQLErrorWithDialect(d Dialect, err error) error {
	if err == nil {
		return nil
	}
	if classifier, ok := d.(ErrorClassifier); ok {
		if classified := classifier.ClassifyError(err); classified != nil {
			if _, ok := classified.(*DataError); ok {
				return classified
			}
			return &DataError{Code: classified, Err: err, Kind: classifyErrorKind(classified, err)}
		}
	}
	if code := sqlErrorCode(err); code != nil {
		return &DataError{Code: code, Err: err, Kind: classifyErrorKind(code, err)}
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "duplicate"), strings.Contains(msg, "unique constraint"), strings.Contains(msg, "duplicate key"):
		return &DataError{Code: ErrDuplicate, Err: err, Kind: "duplicate"}
	case strings.Contains(msg, "foreign key"), strings.Contains(msg, "violates foreign key"):
		return &DataError{Code: ErrForeignKey, Err: err, Kind: "foreign_key"}
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline exceeded"), strings.Contains(msg, "lock wait timeout"):
		return &DataError{Code: ErrTimeout, Err: err, Kind: "timeout"}
	case strings.Contains(msg, "context canceled"), strings.Contains(msg, "query canceled"), strings.Contains(msg, "canceling statement"):
		return &DataError{Code: ErrCanceled, Err: err, Kind: "canceled"}
	case strings.Contains(msg, "not found"), strings.Contains(msg, "no rows"):
		return &DataError{Code: ErrNotFound, Err: err, Kind: "not_found"}
	default:
		return err
	}
}

func classifyErrorKind(code error, err error) string {
	if errors.Is(err, ErrDuplicate) || code == ErrDuplicate {
		return "duplicate"
	}
	if errors.Is(err, ErrForeignKey) || code == ErrForeignKey {
		return "foreign_key"
	}
	if errors.Is(err, ErrTimeout) || code == ErrTimeout {
		return "timeout"
	}
	if errors.Is(err, ErrCanceled) || code == ErrCanceled {
		return "canceled"
	}
	if errors.Is(err, ErrConflict) || code == ErrConflict {
		return "conflict"
	}
	if errors.Is(err, ErrNotFound) || code == ErrNotFound {
		return "not_found"
	}
	if code == ErrInvalidQuery || code == ErrInvalidUpdate || code == ErrInvalidSequence || code == ErrValidation {
		return "validation"
	}
	if code == ErrTxFailed {
		return "tx"
	}
	msg := ""
	if err != nil {
		msg = strings.ToLower(err.Error())
	}
	switch {
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline"):
		return "timeout"
	case strings.Contains(msg, "connection"), strings.Contains(msg, "network"), strings.Contains(msg, "dial tcp"), strings.Contains(msg, "broken pipe"):
		return "driver"
	default:
		return "unknown"
	}
}

func refineErrorCode(code error, err error) error {
	for _, target := range []error{ErrDuplicate, ErrForeignKey, ErrTimeout, ErrCanceled, ErrConflict, ErrNotFound, ErrDriver} {
		if errors.Is(err, target) {
			return target
		}
	}
	if next := sqlErrorCode(err); next != nil {
		return next
	}
	return code
}

func sqlErrorCode(err error) error {
	if err == nil {
		return nil
	}
	if state := sqlState(err); state != "" {
		switch state {
		case "23505":
			return ErrDuplicate
		case "23503":
			return ErrForeignKey
		case "40001", "40P01":
			return ErrConflict
		case "57014":
			return ErrCanceled
		}
		if strings.HasPrefix(state, "08") {
			return ErrDriver
		}
	}
	if number, ok := sqlErrorNumber(err); ok {
		switch number {
		case 1062, 1555, 2067:
			return ErrDuplicate
		case 1451, 1452, 787:
			return ErrForeignKey
		case 1205, 5, 6:
			return ErrTimeout
		case 1213:
			return ErrConflict
		case 1317:
			return ErrCanceled
		case 2006, 2013:
			return ErrDriver
		}
	}
	return nil
}

func sqlState(err error) string {
	type sqlStater interface {
		SQLState() string
	}
	var stater sqlStater
	if errors.As(err, &stater) {
		return strings.TrimSpace(stater.SQLState())
	}
	return reflectStringField(err, "SQLState", "State", "Code")
}

func sqlErrorNumber(err error) (int64, bool) {
	if err == nil {
		return 0, false
	}
	v := reflect.ValueOf(err)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return 0, false
	}
	for _, name := range []string{"Number", "Errno", "Code"} {
		f := v.FieldByName(name)
		if !f.IsValid() {
			continue
		}
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return f.Int(), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(f.Uint()), true
		}
	}
	return 0, false
}

func reflectStringField(err error, names ...string) string {
	if err == nil {
		return ""
	}
	v := reflect.ValueOf(err)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ""
	}
	for _, name := range names {
		f := v.FieldByName(name)
		if !f.IsValid() {
			continue
		}
		switch f.Kind() {
		case reflect.String:
			return strings.TrimSpace(f.String())
		case reflect.Array, reflect.Slice:
			if f.Type().Elem().Kind() == reflect.Uint8 {
				buf := make([]byte, f.Len())
				for i := 0; i < f.Len(); i++ {
					buf[i] = byte(f.Index(i).Uint())
				}
				return strings.TrimSpace(string(buf))
			}
		}
	}
	return ""
}

func ErrorKind(err error) string {
	if err == nil {
		return ""
	}
	if de, ok := err.(*DataError); ok {
		if de.Kind != "" {
			return de.Kind
		}
		return classifyErrorKind(de.Code, de.Err)
	}
	return classifyErrorKind(nil, err)
}
