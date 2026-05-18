package data

import (
	"errors"
	"fmt"
	"testing"
)

type sqlStateTestError struct {
	state string
	text  string
}

func (e sqlStateTestError) Error() string    { return e.text }
func (e sqlStateTestError) SQLState() string { return e.state }

type sqlNumberTestError struct {
	Number uint16
	text   string
}

func (e sqlNumberTestError) Error() string { return e.text }

func TestClassifySQLErrorPromotesSpecificCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want error
		kind string
	}{
		{"postgres duplicate", sqlStateTestError{state: "23505", text: "duplicate key"}, ErrDuplicate, "duplicate"},
		{"postgres foreign key", sqlStateTestError{state: "23503", text: "foreign key"}, ErrForeignKey, "foreign_key"},
		{"mysql duplicate", sqlNumberTestError{Number: 1062, text: "duplicate entry"}, ErrDuplicate, "duplicate"},
		{"mysql timeout", sqlNumberTestError{Number: 1205, text: "lock wait timeout"}, ErrTimeout, "timeout"},
		{"message canceled", fmt.Errorf("context canceled"), ErrCanceled, "canceled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := wrapErr("write", ErrInvalidUpdate, classifySQLError(tc.err))
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected errors.Is(%v), got %v", tc.want, err)
			}
			if got := ErrorKind(err); got != tc.kind {
				t.Fatalf("expected kind %q, got %q", tc.kind, got)
			}
		})
	}
}

func TestDuplicateAndForeignKeyAreConflictCompatible(t *testing.T) {
	for _, code := range []error{ErrDuplicate, ErrForeignKey} {
		err := wrapErr("write", ErrInvalidUpdate, code)
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("expected %v to be conflict-compatible", code)
		}
	}
}
