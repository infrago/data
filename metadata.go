package data

import (
	"strings"

	. "github.com/infrago/base"
)

func lookupField(fields Vars, path string) (Var, bool) {
	if len(fields) == 0 || path == "" {
		return Nil, false
	}
	parts := strings.Split(path, ".")
	item, ok := fields[parts[0]]
	if !ok {
		return Nil, false
	}
	for _, part := range parts[1:] {
		if len(item.Children) == 0 {
			return Nil, false
		}
		next, ok := item.Children[part]
		if !ok {
			return Nil, false
		}
		item = next
	}
	return item, true
}

func cloneVars(in Vars) Vars {
	if len(in) == 0 {
		return Vars{}
	}
	out := Vars{}
	for k, v := range in {
		item := v
		item.Options = cloneMap(v.Options)
		item.Setting = cloneMap(v.Setting)
		item.Children = cloneVars(v.Children)
		out[k] = item
	}
	return out
}

func cloneMap(in Map) Map {
	if len(in) == 0 {
		return Map{}
	}
	out := Map{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
