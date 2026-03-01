package data

import . "github.com/infrago/base"

type (
	Index struct {
		Name   string
		Fields []string
		Unique bool
	}

	Table struct {
		Name    string
		Desc    string
		Schema  string
		Table   string
		Key     string
		Fields  Vars
		Indexes []Index
		Setting Map
	}

	View struct {
		Name    string
		Desc    string
		Schema  string
		View    string
		Key     string
		Fields  Vars
		Setting Map
	}

	Model struct {
		Name    string
		Desc    string
		Schema  string
		Model   string
		Key     string
		Fields  Vars
		Setting Map
	}
)
