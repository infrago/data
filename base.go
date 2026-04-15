package data

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	ScanFunc func(Map) Res
	TxFunc   func(DataBase) error

	MigrateOptions struct {
		Startup     string
		Mode        string
		DryRun      bool
		DiffOnly    bool
		Concurrent  bool
		Timeout     time.Duration
		LockTimeout time.Duration
		Retry       int
		RetryDelay  time.Duration
		Jitter      time.Duration
	}

	PlanOptions struct {
		Enable   bool
		Capacity int
		TTL      time.Duration
	}

	MigrateAction struct {
		Kind   string `json:"kind"`
		Target string `json:"target"`
		SQL    string `json:"sql,omitempty"`
		Apply  bool   `json:"apply"`
		Risk   string `json:"risk,omitempty"`
		Detail string `json:"detail,omitempty"`
	}

	MigrateReport struct {
		Mode    string          `json:"mode"`
		DryRun  bool            `json:"dryRun"`
		Actions []MigrateAction `json:"actions"`
	}

	DataBase interface {
		Close() error
		WithContext(context.Context) DataBase
		WithTimeout(time.Duration) DataBase
		Begin() error
		Commit() error
		Rollback() error
		Tx(TxFunc) error
		TxReadOnly(TxFunc) error
		Migrate(...string)
		MigratePlan(...string) MigrateReport
		MigrateDiff(...string) MigrateReport
		MigrateUp(...string)
		MigrateDown(int)
		MigrateTo(string)
		MigrateDownTo(string)
		Capabilities() Capabilities
		Error() error
		ClearError()
		Sequence(string, int64, int64) (int64, error)
		SequenceMany(string, int64, int64, int64) ([]int64, error)

		Table(string) DataTable
		View(string) DataView
		Model(string) DataModel

		Raw(string, ...Any) []Map
		Exec(string, ...Any) int64
		Parse(...Any) (string, []Any)
	}

	DataTable interface {
		Insert(Map) Map
		InsertMany([]Map) []Map
		Upsert(Map, ...Any) Map
		UpsertMany([]Map, ...Any) []Map
		Update(Map, ...Any) Map
		UpdateMany(Map, ...Any) int64
		Remove(...Any) Map
		RemoveMany(...Any) int64
		Restore(...Any) Map
		RestoreMany(...Any) int64
		Delete(...Any) Map
		DeleteMany(...Any) int64

		Entity(Any) Map
		Count(...Any) int64
		Aggregate(...Any) []Map
		First(...Any) Map
		Query(...Any) []Map
		Scan(ScanFunc, ...Any) Res
		ScanN(int64, ScanFunc, ...Any) Res
		Slice(offset, limit int64, args ...Any) (int64, []Map)
		Group(field string, args ...Any) []Map
	}

	DataView interface {
		Count(...Any) int64
		Aggregate(...Any) []Map
		First(...Any) Map
		Query(...Any) []Map
		Scan(ScanFunc, ...Any) Res
		ScanN(int64, ScanFunc, ...Any) Res
		Slice(offset, limit int64, args ...Any) (int64, []Map)
		Group(field string, args ...Any) []Map
	}

	DataModel interface {
		First(...Any) Map
		Query(...Any) []Map
		Scan(ScanFunc, ...Any) Res
		ScanN(int64, ScanFunc, ...Any) Res
		Slice(offset, limit int64, args ...Any) (int64, []Map)
	}
)

type sqlBase struct {
	inst   *Instance
	conn   Connection
	tx     *sql.Tx
	txDone context.CancelFunc
	closed bool
	mutex  sync.RWMutex
	err    error
	mode   string
	ctx    context.Context
	tmo    time.Duration
	mute   atomic.Int32
}

type invalidDataBase struct {
	err error
}

type invalidTable struct {
	err error
}

const internalSequenceTable = "_infrago_sequences"

var sequenceStoreReady sync.Map

func (m *Module) Base(names ...string) DataBase {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	name := infra.DEFAULT
	if len(names) > 0 && strings.TrimSpace(names[0]) != "" {
		name = names[0]
	}
	inst := m.instances[name]
	if inst == nil {
		return &invalidDataBase{err: errInvalidConnection}
	}
	if provider, ok := inst.conn.(interface{ Base(*Instance) DataBase }); ok {
		return provider.Base(inst)
	}
	return &sqlBase{inst: inst, conn: inst.conn, mode: errorModeFromSetting(inst.Config.Setting)}
}

func (b *sqlBase) Close() error {
	if b.closed {
		return nil
	}
	if b.tx != nil {
		_ = b.tx.Rollback()
		b.tx = nil
	}
	if b.txDone != nil {
		b.txDone()
		b.txDone = nil
	}
	b.closed = true
	return nil
}

func (b *sqlBase) WithContext(ctx context.Context) DataBase {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mutex.Lock()
	b.ctx = ctx
	b.mutex.Unlock()
	return b
}

func (b *sqlBase) WithTimeout(timeout time.Duration) DataBase {
	b.mutex.Lock()
	b.tmo = timeout
	b.mutex.Unlock()
	return b
}

func (b *sqlBase) Error() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	err := b.err
	if b.mode == "auto-clear" {
		b.err = nil
	}
	return err
}

func (b *sqlBase) ClearError() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.err = nil
}

func (b *sqlBase) Sequence(key string, offset, step int64) (int64, error) {
	items, err := b.SequenceMany(key, 1, offset, step)
	if err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	return items[0], nil
}

func (b *sqlBase) SequenceMany(key string, count, offset, step int64) ([]int64, error) {
	if err := b.ensureWritable("sequence"); err != nil {
		b.setError(err)
		return nil, err
	}

	key = strings.TrimSpace(key)
	if key == "" {
		err := wrapErr("sequence", ErrInvalidSequence, errors.New("sequence key is empty"))
		b.setError(err)
		return nil, err
	}
	if count <= 0 {
		err := wrapErr("sequence", ErrInvalidSequence, fmt.Errorf("invalid sequence count: %d", count))
		b.setError(err)
		return nil, err
	}
	if step == 0 {
		step = 1
	}

	if err := b.ensureSequenceStore(); err != nil {
		err = wrapErr("sequence.ensure", ErrDriver, classifySQLError(err))
		b.setError(err)
		return nil, err
	}

	if b.tx != nil {
		items, err := b.sequenceRange(key, count, offset, step)
		if err != nil {
			err = wrapErr("sequence.next", ErrDriver, classifySQLError(err))
			b.setError(err)
			return nil, err
		}
		b.setError(nil)
		return items, nil
	}

	if err := b.beginTx(false); err != nil {
		err = wrapErr("sequence.begin", ErrTxFailed, classifySQLError(err))
		b.setError(err)
		return nil, err
	}

	items, err := b.sequenceRange(key, count, offset, step)
	if err != nil {
		_ = b.Rollback()
		err = wrapErr("sequence.next", ErrDriver, classifySQLError(err))
		b.setError(err)
		return nil, err
	}
	if err := b.Commit(); err != nil {
		_ = b.Rollback()
		err = wrapErr("sequence.commit", ErrTxFailed, classifySQLError(err))
		b.setError(err)
		return nil, err
	}

	b.setError(nil)
	return items, nil
}

func (b *sqlBase) setError(err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if err == nil && b.mode == "sticky" {
		return
	}
	b.err = err
}

func (b *sqlBase) opContext(defaultTimeout time.Duration) (context.Context, context.CancelFunc) {
	b.mutex.RLock()
	base := b.ctx
	timeout := b.tmo
	b.mutex.RUnlock()

	if base == nil {
		base = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	if timeout > 0 {
		return context.WithTimeout(base, timeout)
	}
	return base, func() {}
}

func (b *sqlBase) suppressChange() func() {
	b.mute.Add(1)
	return func() {
		b.mute.Add(-1)
	}
}

func (b *sqlBase) emitChange(op, table string, rows int64, key Any, data Map, where Map) {
	b.emitChangeWithKeys(op, table, rows, key, nil, data, where)
}

func (b *sqlBase) emitChangeWithKeys(op, table string, rows int64, key Any, keys []Any, data Map, where Map) {
	if b == nil || b.inst == nil {
		return
	}
	if b.mute.Load() > 0 {
		return
	}
	if !b.watcherKeysEnabled() {
		keys = nil
	} else if len(keys) == 0 && key != nil {
		keys = []Any{key}
	}
	EmitMutation(b.inst.Name, table, op, rows, key, keys, data, where)
}

func (b *sqlBase) watcherKeysEnabled() bool {
	if b == nil || b.inst == nil || b.inst.Config.Watcher == nil {
		return false
	}
	if v, ok := parseBool(b.inst.Config.Watcher["keys"]); ok {
		return v
	}
	return false
}

func (b *sqlBase) Begin() error {
	return b.beginTx(false)
}

func (b *sqlBase) beginTx(readOnly bool) error {
	if b.tx != nil {
		return nil
	}
	ctx, cancel := b.opContext(10 * time.Second)
	opts := &sql.TxOptions{}
	if readOnly {
		opts.ReadOnly = true
	}
	tx, err := b.conn.DB().BeginTx(ctx, opts)
	if err != nil {
		cancel()
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.begin", ErrTxFailed, classifySQLError(err))
	}
	b.tx = tx
	b.txDone = cancel
	return nil
}

func (b *sqlBase) Commit() error {
	if b.tx == nil {
		return nil
	}
	err := b.tx.Commit()
	b.tx = nil
	if b.txDone != nil {
		b.txDone()
		b.txDone = nil
	}
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.commit", ErrTxFailed, classifySQLError(err))
	}
	return nil
}

func (b *sqlBase) Rollback() error {
	if b.tx == nil {
		return nil
	}
	err := b.tx.Rollback()
	b.tx = nil
	if b.txDone != nil {
		b.txDone()
		b.txDone = nil
	}
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		return wrapErr("tx.rollback", ErrTxFailed, classifySQLError(err))
	}
	return nil
}

func (b *sqlBase) Tx(fn TxFunc) error {
	if fn == nil {
		return nil
	}
	if err := b.beginTx(false); err != nil {
		return wrapErr("tx.begin", ErrTxFailed, err)
	}
	if err := fn(b); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.run", ErrTxFailed, err)
	}
	if err := b.Commit(); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.commit", ErrTxFailed, err)
	}
	return nil
}

func (b *sqlBase) TxReadOnly(fn TxFunc) error {
	if fn == nil {
		return nil
	}
	if b.tx != nil {
		if err := fn(b); err != nil {
			return wrapErr("tx.readonly.run", ErrTxFailed, err)
		}
		return b.Error()
	}
	if err := b.beginTx(true); err != nil {
		return wrapErr("tx.readonly.begin", ErrTxFailed, err)
	}
	if err := fn(b); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.readonly.run", ErrTxFailed, err)
	}
	if err := b.Commit(); err != nil {
		_ = b.Rollback()
		return wrapErr("tx.readonly.commit", ErrTxFailed, err)
	}
	return nil
}

func (b *sqlBase) Capabilities() Capabilities {
	return detectCapabilities(b.conn.Dialect())
}

func (b *sqlBase) Migrate(names ...string) {
	if err := b.ensureWritable("migrate"); err != nil {
		b.setError(err)
		return
	}
	_, _ = b.migrateWith(names, MigrateOptions{})
}

func (b *sqlBase) MigratePlan(names ...string) MigrateReport {
	report, _ := b.migrateWith(names, MigrateOptions{DryRun: true})
	return report
}

func (b *sqlBase) MigrateDiff(names ...string) MigrateReport {
	report, _ := b.migrateWith(names, MigrateOptions{DryRun: true, DiffOnly: true})
	return report
}

func (b *sqlBase) MigrateUp(versions ...string) {
	if err := b.ensureWritable("migrate.up"); err != nil {
		b.setError(err)
		return
	}
	err := b.runVersionedUp(versions...)
	b.setError(err)
}

func (b *sqlBase) MigrateDown(steps int) {
	if err := b.ensureWritable("migrate.down"); err != nil {
		b.setError(err)
		return
	}
	err := b.runVersionedDown(steps)
	b.setError(err)
}

func (b *sqlBase) MigrateTo(version string) {
	if err := b.ensureWritable("migrate.to"); err != nil {
		b.setError(err)
		return
	}
	err := b.runVersionedTo(version)
	b.setError(err)
}

func (b *sqlBase) MigrateDownTo(version string) {
	if err := b.ensureWritable("migrate.downTo"); err != nil {
		b.setError(err)
		return
	}
	err := b.runVersionedDownTo(version)
	b.setError(err)
}

func (b *sqlBase) migrateWith(names []string, override MigrateOptions) (MigrateReport, error) {
	opts := b.defaultMigrateOptions()
	if override.Mode != "" {
		opts.Mode = override.Mode
	}
	if override.DryRun {
		opts.DryRun = true
	}
	if override.DiffOnly {
		opts.DiffOnly = true
		opts.DryRun = true
	}
	if override.Concurrent {
		opts.Concurrent = true
	}
	if override.Timeout > 0 {
		opts.Timeout = override.Timeout
	}
	if override.LockTimeout > 0 {
		opts.LockTimeout = override.LockTimeout
	}
	if override.Retry > 0 {
		opts.Retry = override.Retry
	}
	if override.RetryDelay > 0 {
		opts.RetryDelay = override.RetryDelay
	}
	if override.Jitter > 0 {
		opts.Jitter = override.Jitter
	}
	opts.Mode = normalizeMigrateMode(opts.Mode)

	report := MigrateReport{
		Mode:    opts.Mode,
		DryRun:  opts.DryRun,
		Actions: make([]MigrateAction, 0, 16),
	}

	deadline := time.Now().Add(opts.Timeout)
	if opts.Timeout <= 0 {
		deadline = time.Time{}
	}

	if !opts.DiffOnly && !opts.DryRun {
		if err := b.migrateRetry(opts, "migrate.meta", func() error {
			return b.ensureMigrateMetaTable()
		}); err != nil {
			err = wrapErr("migrate.meta", ErrInvalidQuery, err)
			b.setError(err)
			return report, err
		}
	}

	unlock := func() {}
	if !opts.DryRun {
		u, err := b.acquireMigrateLock(opts)
		if err != nil {
			err = wrapErr("migrate.lock", ErrTxFailed, err)
			b.setError(err)
			return report, err
		}
		unlock = u
	}
	defer unlock()

	targets := names
	explicit := len(targets) > 0
	if len(targets) == 0 {
		targets = make([]string, 0, len(module.tables))
		for name := range module.Tables() {
			targets = append(targets, name)
		}
	}
	sort.Strings(targets)

	for _, name := range targets {
		if !deadline.IsZero() && time.Now().After(deadline) {
			err := wrapErr("migrate.timeout", ErrTxFailed, fmt.Errorf("migrate timeout after %s", opts.Timeout))
			b.setError(err)
			return report, err
		}
		cfg, err := module.tableConfig(b.inst.Name, name)
		if err != nil {
			if explicit {
				err = wrapErr("migrate."+name, ErrInvalidQuery, err)
				b.setError(err)
				return report, err
			}
			continue
		}

		schema := pickSchema(b.inst, cfg.Schema)
		source := pickName(name, cfg.Table)
		actions, err := b.planTableActions(schema, source, pickKey(cfg.Key), cfg.Fields, cfg.Indexes, cfg.Setting, opts)
		if err != nil {
			err = wrapErr("migrate."+name+".plan", ErrInvalidQuery, err)
			b.setError(err)
			return report, err
		}
		report.Actions = append(report.Actions, actions...)
		if !opts.DryRun {
			if err := b.executeMigrateActions(actions, opts); err != nil {
				err = wrapErr("migrate."+name+".apply", ErrInvalidQuery, err)
				b.setError(err)
				return report, err
			}
			cacheTouchTable(b.inst.Name, source)
			if !opts.DiffOnly {
				sig := migrateSignature(cfg)
				if err := b.migrateRetry(opts, "migrate."+name+".mark", func() error { return b.markMigrated(name, sig) }); err != nil {
					err = wrapErr("migrate."+name+".mark", ErrInvalidQuery, err)
					b.setError(err)
					return report, err
				}
			}
		}
	}
	b.setError(nil)
	return report, nil
}

func (b *sqlBase) defaultMigrateOptions() MigrateOptions {
	opts := MigrateOptions{
		Startup:     "off",
		Mode:        "safe",
		DryRun:      false,
		DiffOnly:    false,
		Concurrent:  false,
		Timeout:     5 * time.Minute,
		LockTimeout: 30 * time.Second,
		Retry:       2,
		RetryDelay:  500 * time.Millisecond,
		Jitter:      250 * time.Millisecond,
	}
	if b != nil && b.inst != nil {
		if b.inst.Config.Migrate.Startup != "" {
			opts.Startup = normalizeMigrateStartup(b.inst.Config.Migrate.Startup)
		}
		if b.inst.Config.Migrate.Mode != "" {
			opts.Mode = normalizeMigrateMode(b.inst.Config.Migrate.Mode)
		}
		if b.inst.Config.Migrate.DryRun {
			opts.DryRun = true
		}
		if b.inst.Config.Migrate.DiffOnly {
			opts.DiffOnly = true
			opts.DryRun = true
		}
		if b.inst.Config.Migrate.Concurrent {
			opts.Concurrent = true
		}
		if b.inst.Config.Migrate.Timeout > 0 {
			opts.Timeout = b.inst.Config.Migrate.Timeout
		}
		if b.inst.Config.Migrate.LockTimeout > 0 {
			opts.LockTimeout = b.inst.Config.Migrate.LockTimeout
		}
		if b.inst.Config.Migrate.Retry > 0 {
			opts.Retry = b.inst.Config.Migrate.Retry
		}
		if b.inst.Config.Migrate.RetryDelay > 0 {
			opts.RetryDelay = b.inst.Config.Migrate.RetryDelay
		}
		if b.inst.Config.Migrate.Jitter > 0 {
			opts.Jitter = b.inst.Config.Migrate.Jitter
		}
	}
	return opts
}

func normalizeMigrateMode(mode string) string {
	m := strings.ToLower(strings.TrimSpace(mode))
	switch m {
	case "safe", "strict", "danger":
		return m
	default:
		return "safe"
	}
}

func normalizeMigrateStartup(mode string) string {
	m := strings.ToLower(strings.TrimSpace(mode))
	switch m {
	case "auto", "check", "off", "role":
		return m
	default:
		return "off"
	}
}

func (b *sqlBase) planTableActions(schema, table, key string, fields Vars, indexes []Index, setting Map, opts MigrateOptions) ([]MigrateAction, error) {
	actions := make([]MigrateAction, 0, 8)
	exists, err := b.tableExists(schema, table)
	if err != nil {
		return nil, err
	}

	if !exists {
		sqlText, err := b.buildCreateTableSQL(schema, table, key, fields)
		if err != nil {
			return nil, err
		}
		actions = append(actions, MigrateAction{Kind: "create_table", Target: table, SQL: sqlText, Apply: !opts.DryRun, Risk: migrateActionRisk("create_table")})
	}

	currentCols := map[string]struct{}{}
	if exists {
		currentCols, err = b.loadColumns(schema, table)
		if err != nil {
			return nil, err
		}
	}

	desiredCols := make(map[string]Var, len(fields)+1)
	for name, field := range fields {
		desiredCols[b.storageField(name)] = field
	}
	dbKey := b.storageField(key)
	if _, ok := desiredCols[dbKey]; !ok {
		desiredCols[dbKey] = Var{Type: "int"}
	}

	applyNotNull := false
	if setting != nil {
		if v, ok := setting["migrateNotNullOnAdd"]; ok {
			applyNotNull, _ = parseBool(v)
		}
	}

	names := make([]string, 0, len(desiredCols))
	for name := range desiredCols {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, ok := currentCols[strings.ToLower(name)]; ok {
			continue
		}
		sqlText := b.buildAddColumnSQL(schema, table, name, desiredCols[name], applyNotNull)
		actions = append(actions, MigrateAction{Kind: "add_column", Target: table + "." + name, SQL: sqlText, Apply: !opts.DryRun, Risk: migrateActionRisk("add_column")})
	}

	desiredIndexes := b.collectIndexes(table, indexes, setting)
	currentIndexes, err := b.loadIndexNames(schema, table)
	if err != nil {
		return nil, err
	}
	for _, idx := range desiredIndexes {
		if _, ok := currentIndexes[strings.ToLower(strings.TrimSpace(idx.Name))]; ok {
			continue
		}
		sqlText := b.buildCreateIndexSQL(schema, table, idx, opts.Concurrent)
		actions = append(actions, MigrateAction{Kind: "create_index", Target: idx.Name, SQL: sqlText, Apply: !opts.DryRun, Risk: migrateActionRisk("create_index")})
	}

	if opts.Mode == "strict" || opts.Mode == "danger" {
		for col := range currentCols {
			if _, ok := desiredCols[strings.ToLower(col)]; ok {
				continue
			}
			if strings.EqualFold(col, dbKey) {
				continue
			}
			sqlText := b.buildDropColumnSQL(schema, table, col)
			apply := opts.Mode == "danger" && !opts.DryRun
			detail := ""
			if opts.Mode == "strict" {
				detail = "strict mode reports drift only; use danger mode to apply destructive changes"
			}
			actions = append(actions, MigrateAction{Kind: "drop_column", Target: table + "." + col, SQL: sqlText, Apply: apply, Risk: migrateActionRisk("drop_column"), Detail: detail})
		}
		keep := make(map[string]struct{}, len(desiredIndexes))
		for _, idx := range desiredIndexes {
			keep[strings.ToLower(strings.TrimSpace(idx.Name))] = struct{}{}
		}
		for idx := range currentIndexes {
			if strings.HasPrefix(strings.ToLower(idx), "sqlite_autoindex") {
				continue
			}
			if _, ok := keep[strings.ToLower(idx)]; ok {
				continue
			}
			sqlText := b.buildDropIndexSQL(schema, table, idx)
			apply := opts.Mode == "danger" && !opts.DryRun
			detail := ""
			if opts.Mode == "strict" {
				detail = "strict mode reports drift only; use danger mode to apply destructive changes"
			}
			actions = append(actions, MigrateAction{Kind: "drop_index", Target: idx, SQL: sqlText, Apply: apply, Risk: migrateActionRisk("drop_index"), Detail: detail})
		}
	}

	return actions, nil
}

func (b *sqlBase) executeMigrateActions(actions []MigrateAction, opts MigrateOptions) error {
	for _, action := range actions {
		if !action.Apply {
			continue
		}
		if strings.TrimSpace(action.SQL) == "" {
			continue
		}
		err := b.migrateRetry(opts, "migrate.exec."+action.Kind, func() error {
			_, err := b.currentExec().ExecContext(context.Background(), action.SQL)
			return err
		})
		if err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "duplicate column") {
				continue
			}
			return err
		}
	}
	return nil
}

func migrateActionRisk(kind string) string {
	switch kind {
	case "drop_column":
		return "high"
	case "drop_index":
		return "medium"
	default:
		return "low"
	}
}

func (b *sqlBase) migrateRetry(opts MigrateOptions, tag string, run func() error) error {
	attempts := opts.Retry + 1
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		err := run()
		if err == nil {
			return nil
		}
		last = err
		if i >= attempts-1 || !isTransientMigrateError(err) {
			break
		}
		delay := opts.RetryDelay
		if delay <= 0 {
			delay = 300 * time.Millisecond
		}
		time.Sleep(delay + b.migrateJitter(opts, i))
	}
	return last
}

func isTransientMigrateError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "deadlock"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "temporarily unavailable"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "too many connections"),
		strings.Contains(msg, "database is locked"),
		strings.Contains(msg, "lock wait"):
		return true
	default:
		return false
	}
}

func (b *sqlBase) migrateJitter(opts MigrateOptions, salt int) time.Duration {
	if opts.Jitter <= 0 {
		return 0
	}
	base := b.inst.Name + "|" + strconv.Itoa(salt) + "|" + strconv.FormatInt(time.Now().UnixNano(), 10)
	h := fnvHash(base)
	n := int64(h % uint64(opts.Jitter))
	if n < 0 {
		n = -n
	}
	return time.Duration(n)
}

func fnvHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func (b *sqlBase) acquireMigrateLock(opts MigrateOptions) (func(), error) {
	// Debounce before lock compete in multi-node startup.
	if opts.Jitter > 0 {
		time.Sleep(b.migrateJitter(opts, 99))
	}
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = 30 * time.Second
	}
	deadline := time.Now().Add(lockTimeout)
	dn := strings.ToLower(b.conn.Dialect().Name())
	switch {
	case dn == "pgsql" || dn == "postgres":
		key := int64(fnvHash("infrago:migrate:" + b.inst.Name))
		for {
			var ok bool
			err := b.currentExec().QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1)", key).Scan(&ok)
			if err != nil {
				return nil, err
			}
			if ok {
				return func() { _, _ = b.currentExec().ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key) }, nil
			}
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("migrate lock timeout after %s", lockTimeout)
			}
			time.Sleep(200*time.Millisecond + b.migrateJitter(opts, 1))
		}
	case strings.Contains(dn, "mysql"):
		lockName := "infrago:data:migrate:" + b.inst.Name
		for {
			var ok int
			err := b.currentExec().QueryRowContext(context.Background(), "SELECT GET_LOCK(?, 0)", lockName).Scan(&ok)
			if err != nil {
				return nil, err
			}
			if ok == 1 {
				return func() { _, _ = b.currentExec().ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName) }, nil
			}
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("migrate lock timeout after %s", lockTimeout)
			}
			time.Sleep(200*time.Millisecond + b.migrateJitter(opts, 2))
		}
	default:
		// sqlite / others: no cross-node advisory lock support.
		return func() {}, nil
	}
}

func (b *sqlBase) tableExists(schema, table string) (bool, error) {
	target := b.sourceExpr(schema, table)
	rows, err := b.currentExec().QueryContext(context.Background(), "SELECT * FROM "+target+" WHERE 1=0")
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "no such table") || strings.Contains(msg, "unknown table") {
			return false, nil
		}
		return false, err
	}
	_ = rows.Close()
	return true, nil
}

func (b *sqlBase) buildCreateTableSQL(schema, table, key string, fields Vars) (string, error) {
	d := b.conn.Dialect()
	source := b.sourceExpr(schema, table)
	cols := make([]string, 0, len(fields)+1)
	if key == "" {
		key = "id"
	}
	key = b.storageField(key)
	seenKey := false
	for name, field := range fields {
		col := b.storageField(name)
		sqlType := migrateType(d.Name(), field.Type)
		def := d.Quote(col) + " " + sqlType
		if strings.EqualFold(col, key) {
			seenKey = true
			if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
				def = d.Quote(col) + " INTEGER PRIMARY KEY"
			}
		}
		if field.Required && !field.Nullable {
			def += " NOT NULL"
		}
		def += migrateColumnExtras(d, field)
		cols = append(cols, def)
	}
	if !seenKey {
		if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
			cols = append([]string{d.Quote(key) + " INTEGER PRIMARY KEY"}, cols...)
		} else {
			cols = append([]string{d.Quote(key) + " BIGINT"}, cols...)
		}
	}
	if !strings.Contains(strings.ToLower(d.Name()), "sqlite") {
		cols = append(cols, "PRIMARY KEY ("+d.Quote(key)+")")
	}
	return "CREATE TABLE IF NOT EXISTS " + source + " (" + strings.Join(cols, ",") + ")", nil
}

func (b *sqlBase) buildAddColumnSQL(schema, table, name string, field Var, applyNotNull bool) string {
	d := b.conn.Dialect()
	sqlType := migrateType(d.Name(), field.Type)
	def := d.Quote(name) + " " + sqlType
	if applyNotNull && field.Required && !field.Nullable {
		def += " NOT NULL"
	}
	def += migrateColumnExtras(d, field)
	return "ALTER TABLE " + b.sourceExpr(schema, table) + " ADD COLUMN " + def
}

func migrateColumnExtras(d Dialect, field Var) string {
	out := ""
	if field.Default != nil {
		if lit, ok := migrateDefaultLiteral(field.Default); ok {
			out += " DEFAULT " + lit
		}
	}
	if strings.TrimSpace(field.Collation) != "" {
		out += " COLLATE " + strings.TrimSpace(field.Collation)
	}
	if field.Unique {
		out += " UNIQUE"
	}
	if strings.TrimSpace(field.Check) != "" {
		out += " CHECK (" + strings.TrimSpace(field.Check) + ")"
	}
	if strings.Contains(strings.ToLower(d.Name()), "mysql") && strings.TrimSpace(field.Comment) != "" {
		out += " COMMENT '" + strings.ReplaceAll(field.Comment, "'", "''") + "'"
	}
	return out
}

func migrateDefaultLiteral(v Any) (string, bool) {
	switch vv := v.(type) {
	case nil:
		return "NULL", true
	case string:
		return "'" + strings.ReplaceAll(vv, "'", "''") + "'", true
	case bool:
		if vv {
			return "TRUE", true
		}
		return "FALSE", true
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%v", vv), true
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", vv), true
	case float32, float64:
		return fmt.Sprintf("%v", vv), true
	case time.Time:
		return "'" + vv.Format(time.RFC3339Nano) + "'", true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'", true
	}
}

func (b *sqlBase) collectIndexes(table string, indexes []Index, setting Map) []Index {
	items := make([]Index, 0, len(indexes)+2)
	items = append(items, indexes...)
	if setting != nil {
		if raw, ok := setting["indexes"]; ok {
			switch vv := raw.(type) {
			case []Map:
				for _, one := range vv {
					fields := parseStringList(one["fields"])
					if len(fields) == 0 {
						continue
					}
					name, _ := one["name"].(string)
					unique, _ := parseBool(one["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			case []Any:
				for _, rawOne := range vv {
					one, ok := rawOne.(Map)
					if !ok {
						continue
					}
					fields := parseStringList(one["fields"])
					if len(fields) == 0 {
						continue
					}
					name, _ := one["name"].(string)
					unique, _ := parseBool(one["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			case Map:
				fields := parseStringList(vv["fields"])
				if len(fields) > 0 {
					name, _ := vv["name"].(string)
					unique, _ := parseBool(vv["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			}
		}
	}
	for i := range items {
		if strings.TrimSpace(items[i].Name) == "" {
			items[i].Name = fmt.Sprintf("idx_%s_%d", table, i+1)
		}
	}
	return items
}

func (b *sqlBase) buildCreateIndexSQL(schema, table string, idx Index, concurrent bool) string {
	d := b.conn.Dialect()
	parts := make([]string, 0, len(idx.Fields))
	for _, f := range idx.Fields {
		parts = append(parts, d.Quote(b.storageField(f)))
	}
	target := b.sourceExpr(schema, table)
	sqlText := "CREATE "
	if idx.Unique {
		sqlText += "UNIQUE "
	}
	dn := strings.ToLower(d.Name())
	if concurrent && (dn == "pgsql" || dn == "postgres") {
		sqlText += "INDEX CONCURRENTLY IF NOT EXISTS "
	} else {
		sqlText += "INDEX "
		if !strings.Contains(dn, "mysql") {
			sqlText += "IF NOT EXISTS "
		}
	}
	sqlText += d.Quote(idx.Name) + " ON " + target + " (" + strings.Join(parts, ",") + ")"
	return sqlText
}

func (b *sqlBase) buildDropColumnSQL(schema, table, column string) string {
	return "ALTER TABLE " + b.sourceExpr(schema, table) + " DROP COLUMN " + b.conn.Dialect().Quote(column)
}

func (b *sqlBase) buildDropIndexSQL(schema, table, name string) string {
	dn := strings.ToLower(b.conn.Dialect().Name())
	qn := b.conn.Dialect().Quote(name)
	switch {
	case dn == "pgsql" || dn == "postgres":
		if schema != "" {
			return "DROP INDEX IF EXISTS " + b.conn.Dialect().Quote(schema) + "." + qn
		}
		return "DROP INDEX IF EXISTS " + qn
	case strings.Contains(dn, "mysql"):
		return "DROP INDEX " + qn + " ON " + b.sourceExpr(schema, table)
	default:
		return "DROP INDEX IF EXISTS " + qn
	}
}

func (b *sqlBase) loadIndexNames(schema, table string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	dn := strings.ToLower(b.conn.Dialect().Name())
	switch {
	case dn == "pgsql" || dn == "postgres":
		d := b.conn.Dialect()
		query := "SELECT indexname FROM pg_indexes WHERE schemaname = " + d.Placeholder(1) + " AND tablename = " + d.Placeholder(2)
		schemaName := schema
		if schemaName == "" {
			schemaName = "public"
		}
		rows, err := b.currentExec().QueryContext(context.Background(), query, schemaName, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			out[strings.ToLower(name)] = struct{}{}
		}
		return out, rows.Err()
	case strings.Contains(dn, "mysql"):
		query := "SELECT INDEX_NAME FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? GROUP BY INDEX_NAME"
		rows, err := b.currentExec().QueryContext(context.Background(), query, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			out[strings.ToLower(name)] = struct{}{}
		}
		return out, rows.Err()
	default:
		rows, err := b.currentExec().QueryContext(context.Background(), "PRAGMA index_list("+strconv.Quote(table)+")")
		if err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "no such table") {
				return out, nil
			}
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var seq int
			var name string
			var unique int
			var origin string
			var partial int
			if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				return nil, err
			}
			out[strings.ToLower(name)] = struct{}{}
		}
		return out, rows.Err()
	}
}

func (b *sqlBase) Table(name string) DataTable {
	cfg, err := module.tableConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlTable{sqlView: sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.Table), key: pickKey(cfg.Key), fields: cfg.Fields}}
}

func (b *sqlBase) View(name string) DataView {
	cfg, err := module.viewConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.View), key: pickKey(cfg.Key), fields: cfg.Fields}
}

func (b *sqlBase) Model(name string) DataModel {
	cfg, err := module.modelConfig(b.inst.Name, name)
	if err != nil {
		panic(err)
	}
	return &sqlModel{sqlView: sqlView{base: b, name: name, schema: pickSchema(b.inst, cfg.Schema), source: pickName(name, cfg.Model), key: pickKey(cfg.Key), fields: cfg.Fields}}
}

func pickSchema(inst *Instance, schema string) string {
	if schema != "" {
		return schema
	}
	if inst.Config.Schema != "" {
		return inst.Config.Schema
	}
	return ""
}

func pickName(name, own string) string {
	if own != "" {
		return own
	}
	return strings.ReplaceAll(name, ".", "_")
}

func pickKey(key string) string {
	if key != "" {
		return key
	}
	return "id"
}

func (b *sqlBase) fieldMappingEnabled() bool {
	return b != nil && b.inst != nil && b.inst.Config.Mapping
}

func (b *sqlBase) storageField(field string) string {
	field = strings.TrimSpace(field)
	if field == "" || !b.fieldMappingEnabled() {
		return field
	}
	return SnakeFieldPath(field)
}

func (b *sqlBase) appField(field string) string {
	field = strings.TrimSpace(field)
	if field == "" || !b.fieldMappingEnabled() {
		return field
	}
	return CamelFieldPath(field)
}

func (b *sqlBase) appMap(item Map) Map {
	if item == nil || !b.fieldMappingEnabled() {
		return item
	}
	out := Map{}
	for k, v := range item {
		if strings.HasPrefix(k, "$") {
			out[k] = v
			continue
		}
		out[b.appField(k)] = v
	}
	return out
}

func (b *sqlBase) appMaps(items []Map) []Map {
	if !b.fieldMappingEnabled() || len(items) == 0 {
		return items
	}
	out := make([]Map, 0, len(items))
	for _, item := range items {
		out = append(out, b.appMap(item))
	}
	return out
}

func errorModeFromSetting(setting Map) string {
	mode := "auto-clear"
	if setting == nil {
		return mode
	}
	if raw, ok := setting["errorMode"]; ok {
		if s, ok := raw.(string); ok {
			v := strings.ToLower(strings.TrimSpace(s))
			if v == "sticky" || v == "auto-clear" {
				return v
			}
		}
	}
	return mode
}

func (b *sqlBase) sourceExpr(schema, source string) string {
	d := b.conn.Dialect()
	if schema != "" {
		return d.Quote(schema) + "." + d.Quote(source)
	}
	return d.Quote(source)
}

func (b *sqlBase) currentExec() execer {
	if b.tx != nil {
		return b.tx
	}
	return b.conn.DB()
}

func (b *sqlBase) ensureSequenceStore() error {
	if b == nil || b.inst == nil || b.conn == nil || b.conn.DB() == nil {
		return errInvalidConnection
	}

	cacheKey := sequenceStoreCacheKey(b.inst.Name, b.conn.Dialect().Name())
	if _, ok := sequenceStoreReady.Load(cacheKey); ok {
		return nil
	}

	d := b.conn.Dialect()
	table := d.Quote(internalSequenceTable)
	keyCol := d.Quote("key")
	valCol := d.Quote("value")
	updCol := d.Quote("updated_at")

	query := "CREATE TABLE IF NOT EXISTS " + table + " (" +
		keyCol + " TEXT PRIMARY KEY, " +
		valCol + " BIGINT NOT NULL, " +
		updCol + " BIGINT NOT NULL)"
	if strings.Contains(strings.ToLower(d.Name()), "mysql") {
		query = "CREATE TABLE IF NOT EXISTS " + table + " (" +
			keyCol + " VARCHAR(191) PRIMARY KEY, " +
			valCol + " BIGINT NOT NULL, " +
			updCol + " BIGINT NOT NULL)"
	}

	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()
	if _, err := b.conn.DB().ExecContext(ctx, query); err != nil {
		return err
	}
	sequenceStoreReady.Store(cacheKey, struct{}{})
	return nil
}

func (b *sqlBase) sequenceRange(key string, count, offset, step int64) ([]int64, error) {
	dialect := strings.ToLower(strings.TrimSpace(b.conn.Dialect().Name()))
	switch dialect {
	case "mysql":
		return b.sequenceRangeMySQL(key, count, offset, step)
	default:
		return b.sequenceRangeUpsert(key, count, offset, step)
	}
}

func (b *sqlBase) sequenceRangeUpsert(key string, count, offset, step int64) ([]int64, error) {
	d := b.conn.Dialect()
	table := d.Quote(internalSequenceTable)
	keyCol := d.Quote("key")
	valCol := d.Quote("value")
	updCol := d.Quote("updated_at")
	now := time.Now().Unix()
	initialLast := offset + (count-1)*step
	delta := count * step
	dialect := strings.ToLower(strings.TrimSpace(d.Name()))

	if dialect != "pgsql" && dialect != "postgres" && dialect != "sqlite" {
		return nil, wrapErr("sequence.dialect", ErrUnsupported, fmt.Errorf("unsupported sequence dialect: %s", d.Name()))
	}

	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()

	if dialect == "pgsql" || dialect == "postgres" {
		query := "INSERT INTO " + table + "(" + keyCol + "," + valCol + "," + updCol + ") VALUES($1,$2,$3) " +
			"ON CONFLICT(" + keyCol + ") DO UPDATE SET " + valCol + " = " + table + "." + valCol + " + $4, " + updCol + " = EXCLUDED." + updCol +
			" RETURNING " + valCol + ", (xmax = 0)"
		var last int64
		var inserted bool
		if err := b.currentExec().QueryRowContext(ctx, query, key, initialLast, now, delta).Scan(&last, &inserted); err != nil {
			return nil, err
		}
		if inserted {
			return sequenceItemsFromStart(offset, count, step), nil
		}
		return sequenceItemsFromLast(last, count, step), nil
	}

	query := "UPDATE " + table + " SET " + valCol + " = " + valCol + " + ?, " + updCol + " = ? WHERE " + keyCol + " = ? RETURNING " + valCol
	if _, err := b.currentExec().ExecContext(ctx,
		"INSERT INTO "+table+"("+keyCol+","+valCol+","+updCol+") VALUES("+sequenceInsertPlaceholders(d)+")",
		key, initialLast, now,
	); err == nil {
		return sequenceItemsFromStart(offset, count, step), nil
	} else if !isSequenceConflictErr(err) {
		return nil, err
	}

	var last int64
	if err := b.currentExec().QueryRowContext(ctx, query, delta, now, key).Scan(&last); err != nil {
		return nil, err
	}
	return sequenceItemsFromLast(last, count, step), nil
}

func (b *sqlBase) sequenceRangeMySQL(key string, count, offset, step int64) ([]int64, error) {
	d := b.conn.Dialect()
	table := d.Quote(internalSequenceTable)
	keyCol := d.Quote("key")
	valCol := d.Quote("value")
	updCol := d.Quote("updated_at")
	now := time.Now().Unix()
	initialLast := offset + (count-1)*step
	delta := count * step

	ctx, cancel := b.opContext(10 * time.Second)
	defer cancel()

	insert := "INSERT INTO " + table + "(" + keyCol + "," + valCol + "," + updCol + ") VALUES(?,?,?)"
	if _, err := b.currentExec().ExecContext(ctx, insert, key, initialLast, now); err == nil {
		return sequenceItemsFromStart(offset, count, step), nil
	} else if !isSequenceConflictErr(err) {
		return nil, err
	}

	selectSQL := "SELECT " + valCol + " FROM " + table + " WHERE " + keyCol + " = ? FOR UPDATE"
	var current int64
	if err := b.currentExec().QueryRowContext(ctx, selectSQL, key).Scan(&current); err != nil {
		return nil, err
	}

	next := current + delta
	update := "UPDATE " + table + " SET " + valCol + " = ?, " + updCol + " = ? WHERE " + keyCol + " = ?"
	if _, err := b.currentExec().ExecContext(ctx, update, next, now, key); err != nil {
		return nil, err
	}
	return sequenceItemsFromLast(next, count, step), nil
}

func sequenceStoreCacheKey(name, dialect string) string {
	return strings.TrimSpace(strings.ToLower(name)) + ":" + strings.TrimSpace(strings.ToLower(dialect))
}

func isSequenceConflictErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique") || strings.Contains(msg, "constraint")
}

func sequenceInsertPlaceholders(d Dialect) string {
	switch strings.ToLower(strings.TrimSpace(d.Name())) {
	case "pgsql", "postgres":
		return "$1,$2,$3"
	default:
		return "?,?,?"
	}
}

func sequenceItemsFromStart(start, count, step int64) []int64 {
	items := make([]int64, 0, int(count))
	value := start
	for i := int64(0); i < count; i++ {
		items = append(items, value)
		value += step
	}
	return items
}

func sequenceItemsFromLast(last, count, step int64) []int64 {
	start := last - (count-1)*step
	return sequenceItemsFromStart(start, count, step)
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (b *sqlBase) Raw(query string, args ...Any) []Map {
	start := time.Now()
	ctx, cancel := b.opContext(30 * time.Second)
	defer cancel()
	rows, err := b.currentExec().QueryContext(ctx, query, toInterfaces(args)...)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		b.setError(wrapErr("raw.query", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	defer rows.Close()
	items, err := scanMaps(rows)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		b.setError(wrapErr("raw.scan", ErrInvalidQuery, classifySQLError(err)))
		return nil
	}
	statsFor(b.inst.Name).Queries.Add(1)
	b.logSlow(query, args, start)
	b.setError(nil)
	return items
}

func (b *sqlBase) Exec(query string, args ...Any) int64 {
	if isWriteSQL(query) {
		if err := b.ensureWritable("exec"); err != nil {
			b.setError(err)
			return 0
		}
	}
	start := time.Now()
	ctx, cancel := b.opContext(30 * time.Second)
	defer cancel()
	res, err := b.currentExec().ExecContext(ctx, query, toInterfaces(args)...)
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		b.setError(wrapErr("exec", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	b.logSlow(query, args, start)
	statsFor(b.inst.Name).Writes.Add(1)
	if table, ok := parseWriteSQLTable(query); ok {
		cacheTouchTable(b.inst.Name, table)
	} else {
		cacheVersionBump(b.inst.Name)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		statsFor(b.inst.Name).Errors.Add(1)
		b.setError(wrapErr("exec.rows", ErrInvalidQuery, classifySQLError(err)))
		return 0
	}
	b.setError(nil)
	return affected
}

func (b *sqlBase) Parse(args ...Any) (string, []Any) {
	q, err := ParseQuery(args...)
	if err != nil {
		b.setError(wrapErr("parse.query", ErrInvalidQuery, err))
		return "", nil
	}
	builder := NewSQLBuilder(b.conn.Dialect())
	where, params, err := builder.CompileWhere(q)
	if err != nil {
		b.setError(wrapErr("parse.where", ErrInvalidQuery, err))
		return "", nil
	}
	b.setError(nil)
	return where, params
}

func toInterfaces(args []Any) []any {
	out := make([]any, 0, len(args))
	for _, v := range args {
		out = append(out, v)
	}
	return out
}

func scanMaps(rows *sql.Rows) ([]Map, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	items := make([]Map, 0)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		item := Map{}
		for i, col := range cols {
			switch v := vals[i].(type) {
			case []byte:
				item[col] = string(v)
			default:
				item[col] = v
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func mapCreate(fields Vars, val Map) (Map, error) {
	if len(fields) == 0 {
		out := Map{}
		for k, v := range val {
			out[k] = v
		}
		return out, nil
	}
	out := Map{}
	res := infra.Mapping(fields, val, out, false, false)
	if res != nil && res.Fail() {
		return nil, errors.New(res.Error())
	}
	return out, nil
}

func mapChange(fields Vars, val Map) (Map, error) {
	if len(fields) == 0 {
		out := Map{}
		for k, v := range val {
			out[k] = v
		}
		return out, nil
	}
	out := Map{}
	res := infra.Mapping(fields, val, out, true, false)
	if res != nil && res.Fail() {
		return nil, wrapErr("map.change", ErrInvalidUpdate, fmt.Errorf("%s", res.Error()))
	}
	return out, nil
}

func (b *sqlBase) migrateTable(schema, table, key string, fields Vars, indexes []Index, setting Map) error {
	d := b.conn.Dialect()
	source := b.sourceExpr(schema, table)
	cols := make([]string, 0, len(fields)+1)
	if key == "" {
		key = "id"
	}
	key = b.storageField(key)
	seenKey := false
	for name, field := range fields {
		col := b.storageField(name)
		sqlType := migrateType(d.Name(), field.Type)
		def := d.Quote(col) + " " + sqlType
		if strings.EqualFold(col, key) {
			seenKey = true
			if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
				def = d.Quote(col) + " INTEGER PRIMARY KEY"
			}
		}
		if field.Required && !field.Nullable {
			def += " NOT NULL"
		}
		def += migrateColumnExtras(d, field)
		cols = append(cols, def)
	}
	if !seenKey {
		if strings.Contains(strings.ToLower(d.Name()), "sqlite") {
			cols = append([]string{d.Quote(key) + " INTEGER PRIMARY KEY"}, cols...)
		} else {
			cols = append([]string{d.Quote(key) + " BIGINT"}, cols...)
		}
	}
	if !strings.Contains(strings.ToLower(d.Name()), "sqlite") {
		cols = append(cols, "PRIMARY KEY ("+d.Quote(key)+")")
	}
	sqlText := "CREATE TABLE IF NOT EXISTS " + source + " (" + strings.Join(cols, ",") + ")"
	if _, err := b.currentExec().ExecContext(context.Background(), sqlText); err != nil {
		return err
	}
	if err := b.migrateColumns(schema, table, key, fields, setting); err != nil {
		return err
	}
	return b.migrateIndexes(schema, table, indexes, setting)
}

func (b *sqlBase) migrateColumns(schema, table, key string, fields Vars, setting Map) error {
	exists, err := b.loadColumns(schema, table)
	if err != nil {
		return err
	}
	applyNotNull := false
	if setting != nil {
		if v, ok := setting["migrateNotNullOnAdd"]; ok {
			applyNotNull, _ = parseBool(v)
		}
	}

	merged := make(Vars, len(fields)+1)
	for name, field := range fields {
		merged[b.storageField(name)] = field
	}
	dbKey := b.storageField(key)
	if _, ok := merged[dbKey]; !ok {
		merged[dbKey] = Var{Type: "int"}
	}

	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)

	d := b.conn.Dialect()
	target := b.sourceExpr(schema, table)
	for _, name := range names {
		if _, ok := exists[strings.ToLower(name)]; ok {
			continue
		}
		field := merged[name]
		sqlType := migrateType(d.Name(), field.Type)
		def := d.Quote(name) + " " + sqlType
		if applyNotNull && field.Required && !field.Nullable {
			def += " NOT NULL"
		}
		def += migrateColumnExtras(d, field)
		sqlText := "ALTER TABLE " + target + " ADD COLUMN " + def
		if _, err := b.currentExec().ExecContext(context.Background(), sqlText); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists") {
				continue
			}
			return err
		}
	}
	return nil
}

func (b *sqlBase) loadColumns(schema, table string) (map[string]struct{}, error) {
	target := b.sourceExpr(schema, table)
	rows, err := b.currentExec().QueryContext(context.Background(), "SELECT * FROM "+target+" WHERE 1=0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		out[strings.ToLower(col)] = struct{}{}
	}
	return out, nil
}

func (b *sqlBase) migrateIndexes(schema, table string, indexes []Index, setting Map) error {
	items := make([]Index, 0, len(indexes)+2)
	items = append(items, indexes...)
	// backward compatibility: allow indexes in table.setting
	if setting != nil {
		raw, ok := setting["indexes"]
		if ok {
			switch vv := raw.(type) {
			case []Map:
				for _, one := range vv {
					fields := parseStringList(one["fields"])
					if len(fields) == 0 {
						continue
					}
					name, _ := one["name"].(string)
					unique, _ := parseBool(one["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			case []Any:
				for _, rawOne := range vv {
					one, ok := rawOne.(Map)
					if !ok {
						continue
					}
					fields := parseStringList(one["fields"])
					if len(fields) == 0 {
						continue
					}
					name, _ := one["name"].(string)
					unique, _ := parseBool(one["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			case Map:
				fields := parseStringList(vv["fields"])
				if len(fields) > 0 {
					name, _ := vv["name"].(string)
					unique, _ := parseBool(vv["unique"])
					items = append(items, Index{Name: strings.TrimSpace(name), Fields: fields, Unique: unique})
				}
			}
		}
	}
	if len(items) == 0 {
		return nil
	}
	d := b.conn.Dialect()
	for i, idx := range items {
		fields := idx.Fields
		if len(fields) == 0 {
			continue
		}
		name := strings.TrimSpace(idx.Name)
		if name == "" {
			name = fmt.Sprintf("idx_%s_%d", table, i+1)
		}
		unique := idx.Unique
		parts := make([]string, 0, len(fields))
		for _, f := range fields {
			parts = append(parts, d.Quote(b.storageField(f)))
		}
		target := b.sourceExpr(schema, table)
		sqlText := "CREATE "
		if unique {
			sqlText += "UNIQUE "
		}
		sqlText += "INDEX "
		if !strings.Contains(strings.ToLower(d.Name()), "mysql") {
			sqlText += "IF NOT EXISTS "
		}
		sqlText += d.Quote(name) + " ON " + target + " (" + strings.Join(parts, ",") + ")"
		if _, err := b.currentExec().ExecContext(context.Background(), sqlText); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(strings.ToLower(d.Name()), "mysql") && strings.Contains(msg, "duplicate key name") {
				continue
			}
			if strings.Contains(msg, "already exists") {
				continue
			}
			return err
		}
	}
	return nil
}

func (b *sqlBase) logSlow(query string, args []Any, start time.Time) {
	threshold := time.Duration(0)
	if b.inst != nil && b.inst.Config.Setting != nil {
		if v, ok := b.inst.Config.Setting["slow"]; ok {
			switch vv := v.(type) {
			case int:
				threshold = time.Millisecond * time.Duration(vv)
			case int64:
				threshold = time.Millisecond * time.Duration(vv)
			case float64:
				threshold = time.Millisecond * time.Duration(vv)
			case string:
				if d, err := time.ParseDuration(vv); err == nil {
					threshold = d
				}
			}
		}
	}
	if threshold <= 0 {
		return
	}
	cost := time.Since(start)
	if cost < threshold {
		return
	}
	statsFor(b.inst.Name).Slow.Add(1)
	observeSlowCost(b.inst.Name, cost)
	fmt.Printf("[data][slow] conn=%s dialect=%s cost=%s sql=%s args=%v\n", b.inst.Name, b.conn.Dialect().Name(), cost.String(), query, args)
}

func migrateType(dialect, typ string) string {
	t := strings.ToLower(strings.TrimSpace(typ))
	switch {
	case t == "int" || t == "integer" || t == "int64" || t == "uint" || t == "uint64":
		if strings.Contains(dialect, "sqlite") {
			return "INTEGER"
		}
		return "BIGINT"
	case t == "float" || t == "number" || t == "double" || t == "decimal" || t == "decimal128":
		return "DOUBLE PRECISION"
	case t == "bool":
		return "BOOLEAN"
	case t == "timestamp" || t == "datetime" || t == "date":
		if strings.Contains(dialect, "sqlite") {
			return "DATETIME"
		}
		return "TIMESTAMP"
	case t == "json" || t == "jsonb":
		if strings.Contains(dialect, "mysql") || strings.Contains(dialect, "pgsql") || strings.Contains(dialect, "postgres") {
			return "JSON"
		}
		return "TEXT"
	default:
		return "TEXT"
	}
}

func migrateSignature(cfg Table) string {
	payload, _ := json.Marshal(struct {
		Key     string  `json:"key"`
		Fields  Vars    `json:"fields"`
		Indexes []Index `json:"indexes"`
		Setting Map     `json:"setting"`
	}{
		Key:     cfg.Key,
		Fields:  cfg.Fields,
		Indexes: cfg.Indexes,
		Setting: cfg.Setting,
	})
	sum := sha1.Sum(payload)
	return hex.EncodeToString(sum[:])
}

func (b *sqlBase) ensureMigrateMetaTable() error {
	sqlText := "CREATE TABLE IF NOT EXISTS _infrago_migrations (name TEXT PRIMARY KEY, signature TEXT NOT NULL, updated_at TIMESTAMP NOT NULL)"
	if strings.Contains(strings.ToLower(b.conn.Dialect().Name()), "mysql") {
		sqlText = "CREATE TABLE IF NOT EXISTS _infrago_migrations (`name` VARCHAR(191) PRIMARY KEY, `signature` VARCHAR(64) NOT NULL, `updated_at` TIMESTAMP NOT NULL)"
	}
	_, err := b.currentExec().ExecContext(context.Background(), sqlText)
	return err
}

func (b *sqlBase) migrateAlreadyApplied(name, signature string) (bool, error) {
	d := b.conn.Dialect()
	query := "SELECT signature FROM _infrago_migrations WHERE " + d.Quote("name") + " = " + d.Placeholder(1)
	var current string
	err := b.currentExec().QueryRowContext(context.Background(), query, name).Scan(&current)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return current == signature, nil
}

func (b *sqlBase) markMigrated(name, signature string) error {
	d := strings.ToLower(b.conn.Dialect().Name())
	now := time.Now()
	if d == "pgsql" || d == "postgres" {
		_, err := b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migrations(name,signature,updated_at) VALUES($1,$2,$3) ON CONFLICT(name) DO UPDATE SET signature=EXCLUDED.signature, updated_at=EXCLUDED.updated_at",
			name, signature, now)
		return err
	}
	if d == "sqlite" {
		_, err := b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migrations(name,signature,updated_at) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET signature=excluded.signature, updated_at=excluded.updated_at",
			name, signature, now)
		return err
	}
	_, err := b.currentExec().ExecContext(context.Background(),
		"INSERT INTO _infrago_migrations(name,signature,updated_at) VALUES(?,?,?) ON DUPLICATE KEY UPDATE signature=VALUES(signature), updated_at=VALUES(updated_at)",
		name, signature, now)
	return err
}

func (b *sqlBase) runVersionedUp(versions ...string) error {
	if err := b.ensureVersionMigrateTable(); err != nil {
		return err
	}
	all := module.migrationConfigs(b.inst.Name)
	if len(all) == 0 {
		return nil
	}
	allowed := map[string]struct{}{}
	if len(versions) > 0 {
		for _, v := range versions {
			allowed[strings.TrimSpace(v)] = struct{}{}
		}
	}
	applied, err := b.loadVersionApplied()
	if err != nil {
		return err
	}
	opts := b.defaultMigrateOptions()
	unlock := func() {}
	u, err := b.acquireMigrateLock(opts)
	if err == nil {
		unlock = u
	}
	defer unlock()

	for _, mg := range all {
		if len(allowed) > 0 {
			if _, ok := allowed[mg.Version]; !ok {
				continue
			}
		}
		if meta, ok := applied[mg.Version]; ok {
			if meta != mg.checksum() {
				return fmt.Errorf("migration checksum mismatch: %s", mg.Version)
			}
			continue
		}
		if mg.Up == nil {
			return fmt.Errorf("migration up not defined: %s", mg.Version)
		}
		start := time.Now()
		if err := b.Tx(func(tx DataBase) error { return mg.Up(tx) }); err != nil {
			b.logMigrationEvent("version_up", mg.Version, mg.Name, false, time.Since(start), err)
			return err
		}
		if err := b.markVersionApplied(mg); err != nil {
			b.logMigrationEvent("version_up_mark", mg.Version, mg.Name, false, time.Since(start), err)
			return err
		}
		b.logMigrationEvent("version_up", mg.Version, mg.Name, true, time.Since(start), nil)
	}
	return nil
}

func (b *sqlBase) runVersionedTo(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("empty migrate target version")
	}
	all := module.migrationConfigs(b.inst.Name)
	if len(all) == 0 {
		return nil
	}
	versions := make([]string, 0)
	for _, mg := range all {
		if mg.Version <= target {
			versions = append(versions, mg.Version)
		}
	}
	return b.runVersionedUp(versions...)
}

func (b *sqlBase) runVersionedDown(steps int) error {
	if steps <= 0 {
		steps = 1
	}
	if err := b.ensureVersionMigrateTable(); err != nil {
		return err
	}
	opts := b.defaultMigrateOptions()
	unlock := func() {}
	u, err := b.acquireMigrateLock(opts)
	if err == nil {
		unlock = u
	}
	defer unlock()

	applied, err := b.loadVersionAppliedOrderedDesc()
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		return nil
	}
	mm := map[string]Migration{}
	for _, mg := range module.migrationConfigs(b.inst.Name) {
		mm[mg.Version] = mg
	}
	count := 0
	for _, v := range applied {
		if count >= steps {
			break
		}
		mg, ok := mm[v]
		if !ok {
			return fmt.Errorf("migration version not registered: %s", v)
		}
		if mg.Down == nil {
			return fmt.Errorf("migration down not defined: %s", v)
		}
		start := time.Now()
		if err := b.Tx(func(tx DataBase) error { return mg.Down(tx) }); err != nil {
			b.logMigrationEvent("version_down", mg.Version, mg.Name, false, time.Since(start), err)
			return err
		}
		if err := b.unmarkVersionApplied(v); err != nil {
			b.logMigrationEvent("version_down_unmark", mg.Version, mg.Name, false, time.Since(start), err)
			return err
		}
		b.logMigrationEvent("version_down", mg.Version, mg.Name, true, time.Since(start), nil)
		count++
	}
	return nil
}

func (b *sqlBase) runVersionedDownTo(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return fmt.Errorf("empty migrate down target version")
	}
	all := Migrations(b.inst.Name)
	indexes := map[string]int{}
	targetIdx := -1
	for i, mg := range all {
		v := strings.TrimSpace(mg.Version)
		indexes[v] = i
		if v == target {
			targetIdx = i
		}
	}
	if targetIdx < 0 {
		return fmt.Errorf("migration target version not found: %s", target)
	}
	if err := b.ensureVersionMigrateTable(); err != nil {
		return err
	}
	applied, err := b.loadVersionAppliedOrderedDesc()
	if err != nil {
		return err
	}
	steps := 0
	for _, v := range applied {
		idx, ok := indexes[v]
		if !ok {
			return fmt.Errorf("migration version not registered: %s", v)
		}
		if idx > targetIdx {
			steps++
		}
	}
	if steps <= 0 {
		return nil
	}
	return b.runVersionedDown(steps)
}

func (b *sqlBase) ensureVersionMigrateTable() error {
	sqlText := "CREATE TABLE IF NOT EXISTS _infrago_migrations_v2 (version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TIMESTAMP NOT NULL)"
	if strings.Contains(strings.ToLower(b.conn.Dialect().Name()), "mysql") {
		sqlText = "CREATE TABLE IF NOT EXISTS _infrago_migrations_v2 (`version` VARCHAR(191) PRIMARY KEY, `name` VARCHAR(255) NOT NULL, `checksum` VARCHAR(64) NOT NULL, `applied_at` TIMESTAMP NOT NULL)"
	}
	_, err := b.currentExec().ExecContext(context.Background(), sqlText)
	return err
}

func (b *sqlBase) loadVersionApplied() (map[string]string, error) {
	rows, err := b.currentExec().QueryContext(context.Background(), "SELECT version, checksum FROM _infrago_migrations_v2")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var v, c string
		if err := rows.Scan(&v, &c); err != nil {
			return nil, err
		}
		out[v] = c
	}
	return out, rows.Err()
}

func (b *sqlBase) loadVersionAppliedOrderedDesc() ([]string, error) {
	rows, err := b.currentExec().QueryContext(context.Background(), "SELECT version FROM _infrago_migrations_v2 ORDER BY applied_at DESC, version DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (b *sqlBase) markVersionApplied(mg Migration) error {
	now := time.Now()
	d := strings.ToLower(b.conn.Dialect().Name())
	c := mg.checksum()
	if d == "pgsql" || d == "postgres" {
		_, err := b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migrations_v2(version,name,checksum,applied_at) VALUES($1,$2,$3,$4) ON CONFLICT(version) DO UPDATE SET name=EXCLUDED.name, checksum=EXCLUDED.checksum, applied_at=EXCLUDED.applied_at",
			mg.Version, mg.Name, c, now)
		return err
	}
	if d == "sqlite" {
		_, err := b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migrations_v2(version,name,checksum,applied_at) VALUES(?,?,?,?) ON CONFLICT(version) DO UPDATE SET name=excluded.name, checksum=excluded.checksum, applied_at=excluded.applied_at",
			mg.Version, mg.Name, c, now)
		return err
	}
	_, err := b.currentExec().ExecContext(context.Background(),
		"INSERT INTO _infrago_migrations_v2(version,name,checksum,applied_at) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE name=VALUES(name), checksum=VALUES(checksum), applied_at=VALUES(applied_at)",
		mg.Version, mg.Name, c, now)
	return err
}

func (b *sqlBase) unmarkVersionApplied(version string) error {
	_, err := b.currentExec().ExecContext(context.Background(), "DELETE FROM _infrago_migrations_v2 WHERE version = ?", version)
	if strings.ToLower(b.conn.Dialect().Name()) == "pgsql" || strings.ToLower(b.conn.Dialect().Name()) == "postgres" {
		_, err = b.currentExec().ExecContext(context.Background(), "DELETE FROM _infrago_migrations_v2 WHERE version = $1", version)
	}
	return err
}

func (b *sqlBase) ensureMigrationLogTable() error {
	sqlText := "CREATE TABLE IF NOT EXISTS _infrago_migration_logs (id BIGINT PRIMARY KEY, kind TEXT NOT NULL, version TEXT, name TEXT, success BOOLEAN NOT NULL, cost_ms BIGINT NOT NULL, message TEXT, node TEXT, created_at TIMESTAMP NOT NULL)"
	dn := strings.ToLower(b.conn.Dialect().Name())
	if strings.Contains(dn, "sqlite") {
		sqlText = "CREATE TABLE IF NOT EXISTS _infrago_migration_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, version TEXT, name TEXT, success INTEGER NOT NULL, cost_ms INTEGER NOT NULL, message TEXT, node TEXT, created_at DATETIME NOT NULL)"
	} else if strings.Contains(dn, "mysql") {
		sqlText = "CREATE TABLE IF NOT EXISTS _infrago_migration_logs (`id` BIGINT AUTO_INCREMENT PRIMARY KEY, `kind` VARCHAR(64) NOT NULL, `version` VARCHAR(191), `name` VARCHAR(255), `success` TINYINT(1) NOT NULL, `cost_ms` BIGINT NOT NULL, `message` TEXT, `node` VARCHAR(255), `created_at` TIMESTAMP NOT NULL)"
	}
	_, err := b.currentExec().ExecContext(context.Background(), sqlText)
	return err
}

func (b *sqlBase) ensureWritable(op string) error {
	if b == nil || b.inst == nil {
		return nil
	}
	if b.inst.Config.ReadOnly || isReadOnlySetting(b.inst.Config.Setting) {
		return wrapErr(op, ErrValidation, fmt.Errorf("readonly data connection: %s", b.inst.Name))
	}
	return nil
}

func isReadOnlySetting(setting Map) bool {
	if setting == nil {
		return false
	}
	for _, key := range []string{"readOnly", "readonly"} {
		if raw, ok := setting[key]; ok {
			if vv, ok := parseBool(raw); ok && vv {
				return true
			}
		}
	}
	return false
}

func (b *sqlBase) scanBatchSize() int64 {
	if b == nil || b.inst == nil {
		return 0
	}
	if b.inst.Config.Setting == nil {
		return 0
	}
	for _, key := range []string{"scanBatch", "scan_batch"} {
		if raw, ok := b.inst.Config.Setting[key]; ok {
			if vv, ok := parseInt64(raw); ok && vv > 0 {
				return vv
			}
		}
	}
	return 0
}

func isWriteSQL(query string) bool {
	s := strings.ToLower(strings.TrimSpace(query))
	switch {
	case strings.HasPrefix(s, "insert "),
		strings.HasPrefix(s, "update "),
		strings.HasPrefix(s, "delete "),
		strings.HasPrefix(s, "replace "),
		strings.HasPrefix(s, "merge "),
		strings.HasPrefix(s, "create "),
		strings.HasPrefix(s, "alter "),
		strings.HasPrefix(s, "drop "),
		strings.HasPrefix(s, "truncate "),
		strings.HasPrefix(s, "grant "),
		strings.HasPrefix(s, "revoke "):
		return true
	default:
		return false
	}
}

func parseWriteSQLTable(query string) (string, bool) {
	parts := strings.Fields(strings.ToLower(strings.TrimSpace(query)))
	if len(parts) < 2 {
		return "", false
	}
	switch parts[0] {
	case "insert", "replace":
		// insert into table ...
		if len(parts) >= 3 && parts[1] == "into" {
			return normalizeSQLTableToken(parts[2]), true
		}
	case "update":
		return normalizeSQLTableToken(parts[1]), true
	case "delete":
		// delete from table ...
		if len(parts) >= 3 && parts[1] == "from" {
			return normalizeSQLTableToken(parts[2]), true
		}
	case "truncate":
		// truncate table table ...
		if len(parts) >= 3 && parts[1] == "table" {
			return normalizeSQLTableToken(parts[2]), true
		}
	case "alter", "drop", "create":
		// alter table table ...
		if len(parts) >= 3 && parts[1] == "table" {
			return normalizeSQLTableToken(parts[2]), true
		}
	}
	return "", false
}

func normalizeSQLTableToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.Trim(token, "`\"' ")
	if idx := strings.Index(token, "("); idx >= 0 {
		token = token[:idx]
	}
	if strings.Contains(token, ".") {
		ps := strings.Split(token, ".")
		token = ps[len(ps)-1]
		token = strings.Trim(token, "`\"' ")
	}
	return token
}

func (b *sqlBase) logMigrationEvent(kind, version, name string, success bool, cost time.Duration, err error) {
	if b.ensureMigrationLogTable() != nil {
		return
	}
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	node := migrationNodeID()
	now := time.Now()
	costMS := cost.Milliseconds()
	dn := strings.ToLower(b.conn.Dialect().Name())
	switch {
	case dn == "pgsql" || dn == "postgres":
		_, _ = b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migration_logs(kind,version,name,success,cost_ms,message,node,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
			kind, version, name, success, costMS, msg, node, now)
	case strings.Contains(dn, "mysql"):
		_, _ = b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migration_logs(kind,version,name,success,cost_ms,message,node,created_at) VALUES(?,?,?,?,?,?,?,?)",
			kind, version, name, success, costMS, msg, node, now)
	default:
		ok := 0
		if success {
			ok = 1
		}
		_, _ = b.currentExec().ExecContext(context.Background(),
			"INSERT INTO _infrago_migration_logs(kind,version,name,success,cost_ms,message,node,created_at) VALUES(?,?,?,?,?,?,?,?)",
			kind, version, name, ok, costMS, msg, node, now)
	}
}

func migrationNodeID() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}

func (b *invalidDataBase) Close() error { return nil }
func (b *invalidDataBase) WithContext(context.Context) DataBase {
	return b
}
func (b *invalidDataBase) WithTimeout(time.Duration) DataBase { return b }
func (b *invalidDataBase) Begin() error                       { return b.err }
func (b *invalidDataBase) Commit() error                      { return b.err }
func (b *invalidDataBase) Rollback() error                    { return b.err }
func (b *invalidDataBase) Tx(TxFunc) error                    { return b.err }
func (b *invalidDataBase) TxReadOnly(TxFunc) error            { return b.err }
func (b *invalidDataBase) Migrate(...string)                  {}
func (b *invalidDataBase) MigratePlan(...string) MigrateReport {
	return MigrateReport{}
}
func (b *invalidDataBase) MigrateDiff(...string) MigrateReport {
	return MigrateReport{}
}
func (b *invalidDataBase) MigrateUp(...string)        {}
func (b *invalidDataBase) MigrateDown(int)            {}
func (b *invalidDataBase) MigrateTo(string)           {}
func (b *invalidDataBase) MigrateDownTo(string)       {}
func (b *invalidDataBase) Capabilities() Capabilities { return Capabilities{} }
func (b *invalidDataBase) Error() error               { return b.err }
func (b *invalidDataBase) ClearError()                {}
func (b *invalidDataBase) Sequence(string, int64, int64) (int64, error) {
	return 0, b.err
}
func (b *invalidDataBase) SequenceMany(string, int64, int64, int64) ([]int64, error) {
	return nil, b.err
}
func (b *invalidDataBase) Table(string) DataTable    { return &invalidTable{err: b.err} }
func (b *invalidDataBase) View(string) DataView      { return &invalidTable{err: b.err} }
func (b *invalidDataBase) Model(string) DataModel    { return &invalidTable{err: b.err} }
func (b *invalidDataBase) Raw(string, ...Any) []Map  { return nil }
func (b *invalidDataBase) Exec(string, ...Any) int64 { return 0 }
func (b *invalidDataBase) Parse(...Any) (string, []Any) {
	return "", nil
}

func (t *invalidTable) Insert(Map) Map                 { return nil }
func (t *invalidTable) InsertMany([]Map) []Map         { return nil }
func (t *invalidTable) Upsert(Map, ...Any) Map         { return nil }
func (t *invalidTable) UpsertMany([]Map, ...Any) []Map { return nil }
func (t *invalidTable) Update(Map, ...Any) Map         { return nil }
func (t *invalidTable) UpdateMany(Map, ...Any) int64   { return 0 }
func (t *invalidTable) Remove(...Any) Map              { return nil }
func (t *invalidTable) RemoveMany(...Any) int64        { return 0 }
func (t *invalidTable) Restore(...Any) Map             { return nil }
func (t *invalidTable) RestoreMany(...Any) int64       { return 0 }
func (t *invalidTable) Delete(...Any) Map              { return nil }
func (t *invalidTable) DeleteMany(...Any) int64        { return 0 }
func (t *invalidTable) Entity(Any) Map                 { return nil }
func (t *invalidTable) Count(...Any) int64             { return 0 }
func (t *invalidTable) Aggregate(...Any) []Map         { return nil }
func (t *invalidTable) First(...Any) Map               { return nil }
func (t *invalidTable) Query(...Any) []Map             { return nil }
func (t *invalidTable) Scan(ScanFunc, ...Any) Res      { return infra.Fail.With(t.err.Error()) }
func (t *invalidTable) ScanN(int64, ScanFunc, ...Any) Res {
	return infra.Fail.With(t.err.Error())
}
func (t *invalidTable) Slice(int64, int64, ...Any) (int64, []Map) { return 0, nil }
func (t *invalidTable) Group(string, ...Any) []Map                { return nil }
