package data

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrNotFound        = errors.New("data: not found")
	ErrConflict        = errors.New("data: conflict")
	ErrUnsupported     = errors.New("data: unsupported")
	ErrInvalidQuery    = errors.New("data: invalid query")
	ErrInvalidUpdate   = errors.New("data: invalid update")
	ErrInvalidSequence = errors.New("data: invalid sequence")
	ErrTxFailed        = errors.New("data: tx failed")
	ErrValidation      = errors.New("data: validation")
	ErrDriver          = errors.New("data: driver")
	ErrTimeout         = errors.New("data: timeout")
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
	return target == e.Code
}

func wrapErr(op string, code error, err error) error {
	if err == nil {
		return nil
	}
	return &DataError{Op: op, Code: code, Err: err, Kind: classifyErrorKind(code, err)}
}

func Error(op string, code error, err error) error {
	return wrapErr(op, code, err)
}

func classifySQLError(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "duplicate"), strings.Contains(msg, "unique constraint"), strings.Contains(msg, "duplicate key"):
		return ErrConflict
	case strings.Contains(msg, "not found"), strings.Contains(msg, "no rows"):
		return ErrNotFound
	default:
		return err
	}
}

func classifyErrorKind(code error, err error) string {
	if code == ErrInvalidQuery || code == ErrInvalidUpdate || code == ErrInvalidSequence || code == ErrValidation {
		return "validation"
	}
	if code == ErrTxFailed {
		return "tx"
	}
	if code == ErrConflict {
		return "conflict"
	}
	if code == ErrNotFound {
		return "not_found"
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
