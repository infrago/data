package data

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/infrago/base"
)

type sqlTable struct {
	sqlView
}

func (t *sqlTable) Insert(data Map) Map {
	if err := t.base.ensureWritable(t.name + ".insert"); err != nil {
		t.base.setError(err)
		return nil
	}
	val, err := mapCreate(t.fields, data)
	if err != nil {
		t.base.setError(wrapErr(t.name+".insert.map", ErrInvalidUpdate, err))
		return nil
	}
	val = t.normalizeWriteMap(val)
	if len(val) == 0 {
		t.base.setError(wrapErr(t.name+".insert.empty", ErrInvalidUpdate, fmt.Errorf("empty insert data")))
		return nil
	}

	d := t.base.conn.Dialect()
	keys := make([]string, 0, len(val))
	vals := make([]Any, 0, len(val))
	placeholders := make([]string, 0, len(val))
	for k, v := range val {
		if k == t.key && v == nil {
			continue
		}
		keys = append(keys, d.Quote(t.base.storageField(k)))
		vals = append(vals, v)
		placeholders = append(placeholders, d.Placeholder(len(vals)))
	}

	source := t.base.sourceExpr(t.schema, t.source)
	sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", source, strings.Join(keys, ","), strings.Join(placeholders, ","))

	if d.SupportsReturning() {
		sqlText += " RETURNING " + d.Quote(t.base.storageField(t.key))
		var id any
		if err := t.base.currentExec().QueryRowContext(context.Background(), sqlText, toInterfaces(vals)...).Scan(&id); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			t.base.setError(wrapErr(t.name+".insert.return", ErrInvalidUpdate, classifySQLError(err)))
			return nil
		}
		val[t.key] = id
	} else {
		res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...)
		if err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			t.base.setError(wrapErr(t.name+".insert.exec", ErrInvalidUpdate, classifySQLError(err)))
			return nil
		}
		if id, err := res.LastInsertId(); err == nil {
			val[t.key] = id
		}
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheTouchTable(t.base.inst.Name, t.source)
	out := t.reloadEntityByKey(val[t.key], val)
	t.base.emitChange(MutationInsert, t.source, 1, out[t.key], out, nil)

	t.base.setError(nil)
	return out
}

func (t *sqlTable) InsertMany(items []Map) []Map {
	if err := t.base.ensureWritable(t.name + ".insertMany"); err != nil {
		t.base.setError(err)
		return nil
	}
	if len(items) == 0 {
		t.base.setError(nil)
		return []Map{}
	}
	if out, ok, err := t.insertManyBatch(items); ok {
		t.base.setError(err)
		return out
	}
	out := make([]Map, 0, len(items))
	err := t.base.Tx(func(db DataBase) error {
		tb := db.Table(t.name)
		for _, item := range items {
			one := tb.Insert(item)
			if db.Error() != nil {
				return wrapErr(t.name+".insertMany.item", ErrInvalidUpdate, db.Error())
			}
			out = append(out, one)
		}
		return nil
	})
	t.base.setError(wrapErr(t.name+".insertMany", ErrInvalidUpdate, err))
	return out
}

func (t *sqlTable) insertManyBatch(items []Map) ([]Map, bool, error) {
	normalized := make([]Map, 0, len(items))
	keySet := make(map[string]struct{}, 8)
	for _, item := range items {
		val, err := mapCreate(t.fields, item)
		if err != nil {
			return nil, true, wrapErr(t.name+".insertMany.map", ErrInvalidUpdate, err)
		}
		val = t.normalizeWriteMap(val)
		if len(val) == 0 {
			return nil, true, wrapErr(t.name+".insertMany.empty", ErrInvalidUpdate, fmt.Errorf("empty insert data"))
		}
		normalized = append(normalized, val)
		for k, v := range val {
			if k == t.key && v == nil {
				continue
			}
			keySet[k] = struct{}{}
		}
	}
	if len(keySet) == 0 {
		return nil, true, wrapErr(t.name+".insertMany.cols", ErrInvalidUpdate, fmt.Errorf("no insert columns"))
	}
	cols := make([]string, 0, len(keySet))
	for k := range keySet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	d := t.base.conn.Dialect()
	valuesSQL := make([]string, 0, len(normalized))
	args := make([]Any, 0, len(normalized)*len(cols))
	for _, row := range normalized {
		ph := make([]string, 0, len(cols))
		for _, col := range cols {
			args = append(args, row[col])
			ph = append(ph, d.Placeholder(len(args)))
		}
		valuesSQL = append(valuesSQL, "("+strings.Join(ph, ",")+")")
	}
	quotedCols := make([]string, 0, len(cols))
	for _, col := range cols {
		quotedCols = append(quotedCols, d.Quote(t.base.storageField(col)))
	}
	sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", t.base.sourceExpr(t.schema, t.source), strings.Join(quotedCols, ","), strings.Join(valuesSQL, ","))

	if d.SupportsReturning() {
		sqlText += " RETURNING " + d.Quote(t.base.storageField(t.key))
		rows, err := t.base.currentExec().QueryContext(context.Background(), sqlText, toInterfaces(args)...)
		if err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, true, wrapErr(t.name+".insertMany.returning", ErrInvalidUpdate, classifySQLError(err))
		}
		defer rows.Close()
		i := 0
		for rows.Next() {
			var id any
			if err := rows.Scan(&id); err != nil {
				statsFor(t.base.inst.Name).Errors.Add(1)
				return nil, true, wrapErr(t.name+".insertMany.scan", ErrInvalidUpdate, classifySQLError(err))
			}
			if i < len(normalized) {
				normalized[i][t.key] = id
			}
			i++
		}
		if err := rows.Err(); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, true, wrapErr(t.name+".insertMany.rows", ErrInvalidUpdate, classifySQLError(err))
		}
	} else {
		if _, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(args)...); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			// fallback to per-row in case dialect/driver has edge-case restrictions
			return nil, false, nil
		}
	}
	statsFor(t.base.inst.Name).Writes.Add(int64(len(normalized)))
	cacheTouchTable(t.base.inst.Name, t.source)
	t.base.emitChange(MutationInsert, t.source, int64(len(normalized)), nil, nil, nil)
	return normalized, true, nil
}

func (t *sqlTable) Upsert(data Map, args ...Any) Map {
	if err := t.base.ensureWritable(t.name + ".upsert"); err != nil {
		t.base.setError(err)
		return nil
	}
	condition := Map{}
	if len(args) > 0 {
		if m, ok := args[0].(Map); ok {
			for k, v := range m {
				condition[k] = v
			}
		}
	}
	if len(condition) == 0 {
		if id, ok := data[t.key]; ok && id != nil {
			condition[t.key] = id
		}
	}
	if len(condition) == 0 {
		out := t.Insert(data)
		if t.base.Error() == nil && out != nil {
			t.base.emitChange(MutationUpsert, t.source, 1, out[t.key], nil, condition)
		}
		return out
	}

	createData := t.upsertCreateData(data, condition)
	if item, err := t.upsertNative(createData, condition); err == nil && item != nil {
		item = t.reloadEntityByKey(item[t.key], item)
		t.base.setError(nil)
		t.base.emitChange(MutationUpsert, t.source, 1, item[t.key], nil, condition)
		return item
	}

	item := t.First(condition)
	if t.base.Error() != nil {
		t.base.setError(wrapErr(t.name+".upsert.first", ErrInvalidQuery, t.base.Error()))
		return nil
	}
	if item == nil {
		out := t.Insert(createData)
		if t.base.Error() == nil && out != nil {
			t.base.emitChange(MutationUpsert, t.source, 1, out[t.key], nil, condition)
		}
		return out
	}
	out := t.updateEntity(item, data)
	if t.base.Error() == nil && out != nil {
		t.base.emitChange(MutationUpsert, t.source, 1, out[t.key], nil, condition)
	}
	return out
}

func (t *sqlTable) UpsertMany(items []Map, args ...Any) []Map {
	if err := t.base.ensureWritable(t.name + ".upsertMany"); err != nil {
		t.base.setError(err)
		return nil
	}
	if len(items) == 0 {
		t.base.setError(nil)
		return []Map{}
	}
	out := make([]Map, 0, len(items))
	err := t.base.Tx(func(db DataBase) error {
		tb := db.Table(t.name)
		for _, item := range items {
			one := tb.Upsert(item, args...)
			if db.Error() != nil {
				return wrapErr(t.name+".upsertMany.item", ErrInvalidUpdate, db.Error())
			}
			out = append(out, one)
		}
		return nil
	})
	t.base.setError(wrapErr(t.name+".upsertMany", ErrInvalidUpdate, err))
	return out
}

func (t *sqlTable) updateEntity(item Map, data Map) Map {
	if err := t.base.ensureWritable(t.name + ".change"); err != nil {
		t.base.setError(err)
		return nil
	}
	if item == nil || item[t.key] == nil {
		t.base.setError(wrapErr(t.name+".change.key", ErrInvalidUpdate, fmt.Errorf("missing primary key %s", t.key)))
		return nil
	}

	payload := t.withAutoUpdateStamp(data)
	assigns, vals, err := t.compileAssignments(payload, item, 1)
	if err != nil {
		t.base.setError(wrapErr(t.name+".change.assign", ErrInvalidUpdate, err))
		return nil
	}
	if len(assigns) == 0 {
		t.base.setError(nil)
		return item
	}

	d := t.base.conn.Dialect()
	vals = append(vals, item[t.key])

	sqlText := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s", t.base.sourceExpr(t.schema, t.source), strings.Join(assigns, ","), d.Quote(t.key), d.Placeholder(len(vals)))
	if _, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...); err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".change.exec", ErrInvalidUpdate, classifySQLError(err)))
		return nil
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheTouchTable(t.base.inst.Name, t.source)

	out := Map{}
	for k, v := range item {
		out[k] = v
	}
	setMap, _, _, _, _, _ := t.parseUpdateData(payload, item, true)
	for k, v := range setMap {
		out[k] = v
	}
	out = t.reloadEntityByKey(item[t.key], out)
	t.base.emitChange(MutationUpdate, t.source, 1, item[t.key], setMap, Map{t.key: item[t.key]})
	t.base.setError(nil)
	return out
}

func (t *sqlTable) reloadEntityByKey(id Any, fallback Map) Map {
	if id == nil {
		return fallback
	}
	entity := t.Entity(id)
	if t.base.Error() != nil || entity == nil {
		t.base.setError(nil)
		return fallback
	}
	return entity
}

func (t *sqlTable) Update(sets Map, args ...Any) Map {
	if err := t.base.ensureWritable(t.name + ".update"); err != nil {
		t.base.setError(err)
		return nil
	}
	args = t.singleMutationArgs(args...)
	q, err := ParseQuery(args...)
	if err != nil {
		t.base.setError(wrapErr(t.name+".update.parse", ErrInvalidQuery, err))
		return nil
	}
	q = t.mapQueryToStorage(q)
	q = t.ensureSingleMutationQuery(q)
	items, err := t.queryWithQuery(q)
	if err != nil {
		t.base.setError(wrapErr(t.name+".update.query", ErrInvalidQuery, err))
		return nil
	}
	if len(items) == 0 {
		t.base.setError(nil)
		return nil
	}
	if out := t.updateEntity(items[0], sets); out == nil {
		if t.base.Error() != nil {
			t.base.setError(wrapErr(t.name+".update.change", ErrInvalidUpdate, t.base.Error()))
		}
		return nil
	} else {
		t.base.setError(nil)
		return out
	}
}

func (t *sqlTable) UpdateMany(sets Map, args ...Any) int64 {
	if err := t.base.ensureWritable(t.name + ".update"); err != nil {
		t.base.setError(err)
		return 0
	}
	payload := t.withAutoUpdateStamp(sets)
	q, err := ParseQuery(args...)
	if err != nil {
		t.base.setError(wrapErr(t.name+".update.parse", ErrInvalidQuery, err))
		return 0
	}
	q = t.mapQueryToStorage(q)
	if t.blockUnsafeMutation(q) {
		t.base.setError(wrapErr(t.name+".update.unsafe", ErrInvalidQuery, fmt.Errorf("unsafe update blocked, set %s=true to allow full-table update", OptUnsafe)))
		return 0
	}
	keys, keyErr := t.mutationKeysForQuery(q, t.base.watcherKeysEnabled())
	if keyErr != nil {
		t.base.setError(wrapErr(t.name+".update.keys", ErrInvalidQuery, keyErr))
		return 0
	}
	whereMap := t.queryArgsMap(args...)
	if t.hasCollectionUpdate(payload) {
		affected, err := t.updateByEntityLoop(payload, q)
		t.base.setError(err)
		return affected
	}
	assign, vals, err := t.compileAssignments(payload, nil, 1)
	if err != nil {
		t.base.setError(wrapErr(t.name+".update.assign", ErrInvalidUpdate, err))
		return 0
	}
	if len(assign) == 0 {
		t.base.setError(nil)
		return 0
	}
	d := t.base.conn.Dialect()

	builder := NewSQLBuilder(d)
	t.bindBuilder(builder, q)
	builder.index = len(vals) + 1
	whereSQL, p, err := builder.CompileWhere(q)
	if err != nil {
		t.base.setError(wrapErr(t.name+".update.where", ErrInvalidQuery, err))
		return 0
	}
	for _, arg := range p {
		vals = append(vals, arg)
	}

	sqlText := fmt.Sprintf("UPDATE %s SET %s WHERE %s", t.base.sourceExpr(t.schema, t.source), strings.Join(assign, ","), whereSQL)
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(vals)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".update.exec", ErrInvalidUpdate, classifySQLError(err)))
		return 0
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheTouchTable(t.base.inst.Name, t.source)
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".update.rows", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	var key Any
	if len(keys) > 0 {
		key = keys[0]
	}
	t.base.emitChangeWithKeys(MutationUpdate, t.source, affected, key, keys, payload, whereMap)
	t.base.setError(nil)
	return affected
}

func (t *sqlTable) Delete(args ...Any) Map {
	if err := t.base.ensureWritable(t.name + ".delete"); err != nil {
		t.base.setError(err)
		return nil
	}
	args = t.singleMutationArgs(args...)
	q, err := ParseQuery(args...)
	if err != nil {
		t.base.setError(wrapErr(t.name+".delete.parse", ErrInvalidQuery, err))
		return nil
	}
	q = t.mapQueryToStorage(q)
	q = t.ensureSingleMutationQuery(q)
	items, err := t.queryWithQuery(q)
	if err != nil {
		t.base.setError(wrapErr(t.name+".delete.query", ErrInvalidQuery, err))
		return nil
	}
	if len(items) == 0 {
		t.base.setError(nil)
		return nil
	}
	id := items[0][t.key]
	if id == nil {
		t.base.setError(wrapErr(t.name+".delete.key", ErrInvalidQuery, fmt.Errorf("missing primary key %s", t.key)))
		return nil
	}
	d := t.base.conn.Dialect()
	sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s = %s", t.base.sourceExpr(t.schema, t.source), d.Quote(t.base.storageField(t.key)), d.Placeholder(1))
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, id)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".delete.exec", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".delete.rows", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	if affected > 0 {
		statsFor(t.base.inst.Name).Writes.Add(1)
		cacheTouchTable(t.base.inst.Name, t.source)
		t.base.emitChange(MutationDelete, t.source, affected, id, nil, Map{t.key: id})
	}
	t.base.setError(nil)
	if affected == 0 {
		return nil
	}
	return items[0]
}

func (t *sqlTable) DeleteMany(args ...Any) int64 {
	if err := t.base.ensureWritable(t.name + ".delete"); err != nil {
		t.base.setError(err)
		return 0
	}
	q, err := ParseQuery(args...)
	if err != nil {
		t.base.setError(wrapErr(t.name+".delete.parse", ErrInvalidQuery, err))
		return 0
	}
	q = t.mapQueryToStorage(q)
	if t.blockUnsafeMutation(q) {
		t.base.setError(wrapErr(t.name+".delete.unsafe", ErrInvalidQuery, fmt.Errorf("unsafe delete blocked, set %s=true to allow full-table delete", OptUnsafe)))
		return 0
	}
	keys, keyErr := t.mutationKeysForQuery(q, t.base.watcherKeysEnabled())
	if keyErr != nil {
		t.base.setError(wrapErr(t.name+".delete.keys", ErrInvalidQuery, keyErr))
		return 0
	}
	whereMap := t.queryArgsMap(args...)
	b := NewSQLBuilder(t.base.conn.Dialect())
	t.bindBuilder(b, q)
	whereSQL, params, err := b.CompileWhere(q)
	if err != nil {
		t.base.setError(wrapErr(t.name+".delete.where", ErrInvalidQuery, err))
		return 0
	}
	sqlText := fmt.Sprintf("DELETE FROM %s WHERE %s", t.base.sourceExpr(t.schema, t.source), whereSQL)
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(params)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".delete.exec", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheTouchTable(t.base.inst.Name, t.source)
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		t.base.setError(wrapErr(t.name+".delete.rows", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	var key Any
	if len(keys) > 0 {
		key = keys[0]
	}
	t.base.emitChangeWithKeys(MutationDelete, t.source, affected, key, keys, nil, whereMap)
	t.base.setError(nil)
	return affected
}

func (t *sqlTable) ensureSingleMutationQuery(q Query) Query {
	if len(q.Sort) == 0 {
		key := strings.TrimSpace(t.base.storageField(t.key))
		if key != "" {
			q.Sort = []Sort{{Field: key}}
		}
	}
	q.Offset = 0
	q.Limit = 1
	return q
}

func (t *sqlTable) singleMutationArgs(args ...Any) []Any {
	id, ok := t.pickPrimaryValue(args...)
	if !ok {
		return args
	}
	return []Any{Map{t.key: id}}
}

func (t *sqlTable) pickPrimaryValue(args ...Any) (Any, bool) {
	if len(args) == 0 {
		return nil, false
	}
	storageKey := t.base.storageField(t.key)
	for _, arg := range args {
		m, ok := arg.(Map)
		if !ok || len(m) == 0 {
			continue
		}
		if v, ok := m[t.key]; ok && v != nil {
			return v, true
		}
		if storageKey != t.key {
			if v, ok := m[storageKey]; ok && v != nil {
				return v, true
			}
		}
	}
	return nil, false
}

func (t *sqlTable) mutationKeysForQuery(q Query, enabled bool) ([]Any, error) {
	if !enabled {
		return nil, nil
	}
	qq := q
	qq.Select = []string{t.key}
	qq.Offset = 0
	if len(qq.Sort) == 0 {
		key := strings.TrimSpace(t.base.storageField(t.key))
		if key != "" {
			qq.Sort = []Sort{{Field: key}}
		}
	}
	qq.Limit = 0
	items, err := t.queryWithQuery(qq)
	if err != nil {
		return nil, err
	}
	keys := make([]Any, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if id, ok := item[t.key]; ok && id != nil {
			keys = append(keys, id)
		}
	}
	return keys, nil
}

func (t *sqlTable) queryArgsMap(args ...Any) Map {
	where := Map{}
	for _, arg := range args {
		m, ok := arg.(Map)
		if !ok {
			continue
		}
		for k, v := range m {
			where[k] = v
		}
	}
	return where
}

func (t *sqlTable) Entity(id Any) Map {
	return t.First(Map{t.key: id})
}

func (t *sqlTable) compileAssignments(input Map, current Map, start int) ([]string, []Any, error) {
	d := t.base.conn.Dialect()
	setPart, incPart, unsetPart, pathPart, unsetPathPart, err := t.parseUpdateData(input, current, current != nil)
	if err != nil {
		return nil, nil, err
	}

	setVal, err := mapChange(t.fields, setPart)
	if err != nil {
		return nil, nil, err
	}
	setVal = t.normalizeWriteMap(setVal)

	clauses := make([]string, 0, len(setVal)+len(incPart)+len(unsetPart)+len(pathPart)+len(unsetPathPart))
	vals := make([]Any, 0, len(setVal)+len(incPart)+len(pathPart)*2+len(unsetPathPart))
	placeholderIdx := start

	for k, v := range setVal {
		if k == t.key {
			continue
		}
		vals = append(vals, v)
		clauses = append(clauses, d.Quote(t.base.storageField(k))+" = "+d.Placeholder(placeholderIdx))
		placeholderIdx++
	}

	for k, v := range incPart {
		if k == t.key {
			continue
		}
		field := strings.TrimSpace(k)
		if field == "" {
			continue
		}
		if strings.Contains(field, ".") {
			parts := strings.Split(field, ".")
			root := strings.TrimSpace(parts[0])
			path := make([]string, 0, len(parts)-1)
			for _, seg := range parts[1:] {
				seg = strings.TrimSpace(seg)
				if seg != "" {
					path = append(path, seg)
				}
			}
			if root != "" && len(path) > 0 && t.isJSONField(root) {
				segmentPath := strings.Join(path, ".")
				fieldExpr := d.Quote(t.base.storageField(root))
				name := strings.ToLower(d.Name())
				switch {
				case name == "pgsql" || name == "postgres":
					vals = append(vals, "{"+strings.Join(path, ",")+"}", v)
					pathPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					incPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					clauses = append(clauses, fieldExpr+" = jsonb_set(COALESCE("+fieldExpr+", '{}'::jsonb), "+pathPH+"::text[], to_jsonb(COALESCE((COALESCE("+fieldExpr+", '{}'::jsonb)#>>"+pathPH+"::text[])::numeric,0) + "+incPH+"::numeric), true)")
				case name == "mysql":
					vals = append(vals, "$."+segmentPath, v)
					pathPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					incPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					clauses = append(clauses, fieldExpr+" = JSON_SET(COALESCE("+fieldExpr+", JSON_OBJECT()), "+pathPH+", CAST((CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(COALESCE("+fieldExpr+", JSON_OBJECT()), "+pathPH+")), '0') AS DECIMAL(65,10)) + CAST("+incPH+" AS DECIMAL(65,10))) AS JSON))")
				case name == "sqlite":
					vals = append(vals, "$."+segmentPath, v)
					pathPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					incPH := d.Placeholder(placeholderIdx)
					placeholderIdx++
					clauses = append(clauses, fieldExpr+" = json_set(COALESCE("+fieldExpr+", '{}'), "+pathPH+", (COALESCE(CAST(json_extract(COALESCE("+fieldExpr+", '{}'), "+pathPH+") AS REAL),0) + CAST("+incPH+" AS REAL)))")
				default:
					return nil, nil, wrapErr("update.incPath", ErrUnsupported, fmt.Errorf("dialect %s does not support %s on json path", d.Name(), UpdInc))
				}
				continue
			}
		}
		vals = append(vals, v)
		f := d.Quote(t.base.storageField(field))
		clauses = append(clauses, f+" = COALESCE("+f+",0) + "+d.Placeholder(placeholderIdx))
		placeholderIdx++
	}

	for _, k := range unsetPart {
		if k == t.key {
			continue
		}
		clauses = append(clauses, d.Quote(t.base.storageField(k))+" = NULL")
	}

	for path, value := range pathPart {
		parts := strings.Split(path, ".")
		if len(parts) < 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		if field == "" || field == t.key {
			continue
		}
		segments := make([]string, 0, len(parts)-1)
		for _, seg := range parts[1:] {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				segments = append(segments, seg)
			}
		}
		if len(segments) == 0 {
			continue
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, nil, err
		}
		fieldExpr := d.Quote(t.base.storageField(field))
		name := strings.ToLower(d.Name())
		switch {
		case name == "pgsql" || name == "postgres":
			vals = append(vals, "{"+strings.Join(segments, ",")+"}", string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = jsonb_set(COALESCE("+fieldExpr+", '{}'::jsonb), "+pathPH+"::text[], "+valPH+"::jsonb, true)")
		case name == "mysql":
			vals = append(vals, "$."+strings.Join(segments, "."), string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = JSON_SET(COALESCE("+fieldExpr+", JSON_OBJECT()), "+pathPH+", CAST("+valPH+" AS JSON))")
		case name == "sqlite":
			vals = append(vals, "$."+strings.Join(segments, "."), string(raw))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			valPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = json_set(COALESCE("+fieldExpr+", '{}'), "+pathPH+", json("+valPH+"))")
		default:
			return nil, nil, wrapErr("update.path", ErrUnsupported, fmt.Errorf("dialect %s does not support %s", d.Name(), UpdSetPath))
		}
	}
	for _, path := range unsetPathPart {
		parts := strings.Split(path, ".")
		if len(parts) < 2 {
			continue
		}
		field := strings.TrimSpace(parts[0])
		if field == "" || field == t.key {
			continue
		}
		segments := make([]string, 0, len(parts)-1)
		for _, seg := range parts[1:] {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				segments = append(segments, seg)
			}
		}
		if len(segments) == 0 {
			continue
		}
		fieldExpr := d.Quote(t.base.storageField(field))
		name := strings.ToLower(d.Name())
		switch {
		case name == "pgsql" || name == "postgres":
			vals = append(vals, "{"+strings.Join(segments, ",")+"}")
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = COALESCE("+fieldExpr+", '{}'::jsonb) #- "+pathPH+"::text[]")
		case name == "mysql":
			vals = append(vals, "$."+strings.Join(segments, "."))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = JSON_REMOVE(COALESCE("+fieldExpr+", JSON_OBJECT()), "+pathPH+")")
		case name == "sqlite":
			vals = append(vals, "$."+strings.Join(segments, "."))
			pathPH := d.Placeholder(placeholderIdx)
			placeholderIdx++
			clauses = append(clauses, fieldExpr+" = json_remove(COALESCE("+fieldExpr+", '{}'), "+pathPH+")")
		default:
			return nil, nil, wrapErr("update.unsetPath", ErrUnsupported, fmt.Errorf("dialect %s does not support %s", d.Name(), UpdUnsetPath))
		}
	}

	return clauses, vals, nil
}

func (t *sqlTable) parseUpdateData(input Map, current Map, allowCollection bool) (Map, Map, []string, Map, []string, error) {
	setPart := Map{}
	incPart := Map{}
	unsetPart := make([]string, 0)
	pathPart := Map{}
	unsetPathPart := make([]string, 0)
	hasCollection := false

	for k, v := range input {
		switch k {
		case UpdSet:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					setPart[kk] = vv
				}
			}
		case UpdInc:
			if m, ok := v.(Map); ok {
				for kk, vv := range m {
					incPart[kk] = vv
				}
			}
		case UpdUnset:
			switch vv := v.(type) {
			case string:
				if strings.TrimSpace(vv) != "" {
					unsetPart = append(unsetPart, strings.TrimSpace(vv))
				}
			case []string:
				for _, field := range vv {
					field = strings.TrimSpace(field)
					if field != "" {
						unsetPart = append(unsetPart, field)
					}
				}
			case []Any:
				for _, item := range vv {
					if field, ok := item.(string); ok {
						field = strings.TrimSpace(field)
						if field != "" {
							unsetPart = append(unsetPart, field)
						}
					}
				}
			case Map:
				for field, on := range vv {
					if yes, ok := parseBool(on); ok && yes {
						unsetPart = append(unsetPart, field)
					}
				}
			}
		case UpdSetPath:
			if m, ok := v.(Map); ok {
				for path, value := range m {
					path = strings.TrimSpace(path)
					if path != "" {
						pathPart[path] = value
					}
				}
			}
		case UpdUnsetPath:
			switch vv := v.(type) {
			case string:
				vv = strings.TrimSpace(vv)
				if vv != "" {
					unsetPathPart = append(unsetPathPart, vv)
				}
			case []string:
				for _, one := range vv {
					one = strings.TrimSpace(one)
					if one != "" {
						unsetPathPart = append(unsetPathPart, one)
					}
				}
			case []Any:
				for _, one := range vv {
					if s, ok := one.(string); ok {
						s = strings.TrimSpace(s)
						if s != "" {
							unsetPathPart = append(unsetPathPart, s)
						}
					}
				}
			}
		case UpdPush, UpdPull, UpdAddToSet:
			hasCollection = true
		default:
			if !strings.HasPrefix(k, "$") {
				setPart[k] = v
			}
		}
	}

	if hasCollection && !allowCollection {
		return nil, nil, nil, nil, nil, wrapErr("update.collection", ErrUnsupported, fmt.Errorf("%s/%s/%s needs entity context", UpdPush, UpdPull, UpdAddToSet))
	}

	setPart = t.applyCollectionUpdates(setPart, current, input)
	return setPart, incPart, unsetPart, pathPart, unsetPathPart, nil
}

func (t *sqlTable) applyCollectionUpdates(setPart Map, current Map, input Map) Map {
	if raw, ok := input[UpdPush].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionAppend(base, value, false)
		}
	}
	if raw, ok := input[UpdAddToSet].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionAppend(base, value, true)
		}
	}
	if raw, ok := input[UpdPull].(Map); ok {
		for field, value := range raw {
			base := setPart[field]
			if base == nil && current != nil {
				base = current[field]
			}
			setPart[field] = collectionRemove(base, value)
		}
	}
	return setPart
}

func (t *sqlTable) normalizeWriteMap(input Map) Map {
	out := Map{}
	for k, v := range input {
		out[k] = t.normalizeWriteValue(k, v)
	}
	return out
}

func (t *sqlTable) normalizeWriteValue(field string, value Any) Any {
	cfg, hasField := t.fields[field]
	typeName := ""
	if hasField {
		typeName = strings.ToLower(strings.TrimSpace(cfg.Type))
	}
	dialect := strings.ToLower(t.base.conn.Dialect().Name())

	if typeName == "json" || typeName == "jsonb" || strings.HasPrefix(typeName, "map") {
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	if strings.HasPrefix(typeName, "array") || strings.HasPrefix(typeName, "[]") {
		if dialect == "pgsql" || dialect == "postgres" {
			return pgArrayLiteral(value)
		}
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	// Default: marshal map/slice into JSON string for portability.
	switch value.(type) {
	case Map, []Map, []Any, []string, []int, []int64, []float64, []float32, []bool:
		if b, err := json.Marshal(value); err == nil {
			return string(b)
		}
	}

	return value
}

func collectionAppend(current Any, appendVal Any, unique bool) Any {
	arr := toAnySlice(current)
	for _, v := range toAnySlice(appendVal) {
		if unique && containsAny(arr, v) {
			continue
		}
		arr = append(arr, v)
	}
	return arr
}

func collectionRemove(current Any, removeVal Any) Any {
	arr := toAnySlice(current)
	remove := toAnySlice(removeVal)
	out := make([]Any, 0, len(arr))
	for _, item := range arr {
		if containsAny(remove, item) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func containsAny(items []Any, value Any) bool {
	target := fmt.Sprintf("%v", value)
	for _, item := range items {
		if fmt.Sprintf("%v", item) == target {
			return true
		}
	}
	return false
}

func toAnySlice(v Any) []Any {
	switch vv := v.(type) {
	case nil:
		return []Any{}
	case []Any:
		return vv
	case []string:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []int64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []float64:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	case []Map:
		out := make([]Any, 0, len(vv))
		for _, one := range vv {
			out = append(out, one)
		}
		return out
	default:
		return []Any{v}
	}
}

func (t *sqlTable) upsertNative(data Map, condition Map) (Map, error) {
	if len(condition) != 1 {
		return nil, nil
	}
	d := t.base.conn.Dialect()
	name := strings.ToLower(d.Name())
	if name != "pgsql" && name != "postgres" && name != "mysql" && name != "sqlite" {
		return nil, nil
	}

	var conflictKey string
	var conflictVal Any
	for k, v := range condition {
		conflictKey = k
		conflictVal = v
	}

	merged := Map{}
	for k, v := range data {
		merged[k] = v
	}
	merged = t.withAutoUpdateStamp(merged)
	if _, ok := merged[conflictKey]; !ok {
		merged[conflictKey] = conflictVal
	}

	createVal, err := mapCreate(t.fields, merged)
	if err != nil {
		return nil, wrapErr(t.name+".upsert.native.map", ErrInvalidUpdate, err)
	}
	if len(createVal) == 0 {
		return nil, wrapErr(t.name+".upsert.native.empty", ErrInvalidUpdate, fmt.Errorf("empty upsert data"))
	}

	keys := make([]string, 0, len(createVal))
	for k := range createVal {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	insertCols := make([]string, 0, len(keys))
	insertPH := make([]string, 0, len(keys))
	insertVals := make([]Any, 0, len(keys))
	for _, k := range keys {
		insertCols = append(insertCols, d.Quote(t.base.storageField(k)))
		insertVals = append(insertVals, createVal[k])
		insertPH = append(insertPH, d.Placeholder(len(insertVals)))
	}

	updateCols := make([]string, 0, len(keys))
	for _, k := range keys {
		if k == conflictKey {
			continue
		}
		qk := d.Quote(t.base.storageField(k))
		switch name {
		case "pgsql", "postgres":
			updateCols = append(updateCols, qk+" = EXCLUDED."+qk)
		case "sqlite":
			updateCols = append(updateCols, qk+" = excluded."+qk)
		case "mysql":
			updateCols = append(updateCols, qk+" = VALUES("+qk+")")
		}
	}
	if len(updateCols) == 0 {
		ck := d.Quote(t.base.storageField(conflictKey))
		updateCols = append(updateCols, ck+" = "+ck)
	}

	source := t.base.sourceExpr(t.schema, t.source)
	sqlText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", source, strings.Join(insertCols, ","), strings.Join(insertPH, ","))
	switch name {
	case "pgsql", "postgres":
		sqlText += " ON CONFLICT (" + d.Quote(t.base.storageField(conflictKey)) + ") DO UPDATE SET " + strings.Join(updateCols, ",")
		sqlText += " RETURNING " + d.Quote(t.base.storageField(t.key))
	case "sqlite":
		sqlText += " ON CONFLICT(" + d.Quote(t.base.storageField(conflictKey)) + ") DO UPDATE SET " + strings.Join(updateCols, ",")
		if d.SupportsReturning() {
			sqlText += " RETURNING " + d.Quote(t.base.storageField(t.key))
		}
	case "mysql":
		sqlText += " ON DUPLICATE KEY UPDATE " + strings.Join(updateCols, ",")
	}

	out := Map{}
	for k, v := range createVal {
		out[k] = v
	}
	if d.SupportsReturning() {
		var id any
		if err := t.base.currentExec().QueryRowContext(context.Background(), sqlText, toInterfaces(insertVals)...).Scan(&id); err != nil {
			statsFor(t.base.inst.Name).Errors.Add(1)
			return nil, wrapErr(t.name+".upsert.native.return", ErrInvalidUpdate, classifySQLError(err))
		}
		out[t.key] = id
		statsFor(t.base.inst.Name).Writes.Add(1)
		cacheTouchTable(t.base.inst.Name, t.source)
		return out, nil
	}
	res, err := t.base.currentExec().ExecContext(context.Background(), sqlText, toInterfaces(insertVals)...)
	if err != nil {
		statsFor(t.base.inst.Name).Errors.Add(1)
		return nil, wrapErr(t.name+".upsert.native.exec", ErrInvalidUpdate, classifySQLError(err))
	}
	if id, err := res.LastInsertId(); err == nil && id > 0 {
		out[t.key] = id
	}
	statsFor(t.base.inst.Name).Writes.Add(1)
	cacheTouchTable(t.base.inst.Name, t.source)
	return out, nil
}

func (t *sqlTable) upsertCreateData(data Map, condition Map) Map {
	setPart, incPart, _, _, _, _ := t.parseUpdateData(data, nil, false)
	out := Map{}
	for k, v := range condition {
		out[k] = v
	}
	for k, v := range setPart {
		out[k] = v
	}
	// Mongo-style insert behavior for $inc in upsert: initialize with inc value.
	for k, v := range incPart {
		if _, exists := out[k]; !exists {
			out[k] = v
		}
	}
	return out
}

func (t *sqlTable) blockUnsafeMutation(q Query) bool {
	if q.Unsafe {
		return false
	}
	if !t.safeWriteEnabled() {
		return false
	}
	if q.RawWhere != "" {
		return false
	}
	_, isTrue := q.Filter.(TrueExpr)
	return isTrue
}

func (t *sqlTable) safeWriteEnabled() bool {
	if t.base == nil || t.base.inst == nil || t.base.inst.Config.Setting == nil {
		return true
	}
	raw, ok := t.base.inst.Config.Setting["safeWrite"]
	if !ok {
		return true
	}
	on, yes := parseBool(raw)
	if yes {
		return on
	}
	return true
}

func (t *sqlTable) hasCollectionUpdate(input Map) bool {
	if input == nil {
		return false
	}
	_, push := input[UpdPush]
	_, pull := input[UpdPull]
	_, add := input[UpdAddToSet]
	return push || pull || add
}

func (t *sqlTable) updateByEntityLoop(sets Map, q Query) (int64, error) {
	items, err := t.sqlView.queryWithQuery(q)
	if err != nil {
		return 0, wrapErr(t.name+".update.loop.query", ErrInvalidQuery, err)
	}
	if len(items) == 0 {
		return 0, nil
	}
	affected := int64(0)
	err = t.base.Tx(func(db DataBase) error {
		tb := db.Table(t.name)
		for _, item := range items {
			sqltb, ok := tb.(*sqlTable)
			if !ok {
				return wrapErr(t.name+".update.loop.table", ErrInvalidUpdate, fmt.Errorf("invalid table type %T", tb))
			}
			_ = sqltb.updateEntity(item, sets)
			if db.Error() != nil {
				return db.Error()
			}
			affected++
		}
		return nil
	})
	return affected, err
}

func (t *sqlTable) withAutoUpdateStamp(input Map) Map {
	field := t.autoUpdateFieldName()
	if field == "" {
		return input
	}
	out := Map{}
	for k, v := range input {
		out[k] = v
	}
	now := time.Now()
	if rawSet, ok := out[UpdSet].(Map); ok && rawSet != nil {
		setMap := Map{}
		for k, v := range rawSet {
			setMap[k] = v
		}
		setMap[field] = now
		out[UpdSet] = setMap
		return out
	}
	out[field] = now
	return out
}

func (t *sqlTable) autoUpdateFieldName() string {
	if len(t.fields) == 0 {
		return ""
	}
	candidates := []string{"updatedAt", "changed", "updated_at"}
	for _, name := range candidates {
		if _, ok := t.fields[name]; ok {
			return name
		}
	}
	for key := range t.fields {
		k := strings.ToLower(strings.TrimSpace(key))
		if k == "updatedat" || k == "changed" || k == "updated_at" {
			return key
		}
	}
	return ""
}
