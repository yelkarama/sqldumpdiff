package comparer

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/younes/sqldumpdiff/internal/logger"
	"github.com/younes/sqldumpdiff/internal/parser"
)

// ComparisonResult holds the results of comparing a single table
type ComparisonResult struct {
	TableName   string
	Changes     string
	Deletions   string
	InsertCount int
	UpdateCount int
	DeleteCount int
}

// TableTiming captures per-table timings for profiling.
type TableTiming struct {
	Table   string
	LoadMS  int64
	CompMS  int64
	DelMS   int64
}

func (t TableTiming) TotalMS() int64 {
	return t.LoadMS + t.CompMS + t.DelMS
}

// Summary holds aggregate counts for delta generation.
type Summary struct {
	InsertCount int
	UpdateCount int
	DeleteCount int
}

// TimingMeta captures overall timings and per-table timings for JSON reporting.
type TimingMeta struct {
	SplitMS   int64
	CompareMS int64
	DeleteMS  int64
	WriteMS   int64
	WriteFormatMS int64
	WriteIOMS     int64
	Tables    []*TableTiming
}

// SQLite tuning knobs (defaults are overridden by CLI profiles/flags).
// These are package-level so the CLI can update them once at startup.
var sqliteBatchSize = 20000
var sqliteCacheKB = 800000
var sqliteMmapMB = 128
var sqliteWorkers = 0
var timingEnabled = false

// rowHash computes a deterministic hash of a row using column order.
// We hash a canonical "col=value" sequence to quickly detect unchanged rows.
func rowHash(row *parser.InsertRow) []byte {
	var b strings.Builder
	for _, col := range row.Columns {
		b.WriteString(strings.ToLower(col))
		b.WriteString("=")
		val := getColumnValueCaseInsensitive(row, col)
		b.WriteString(normalizeNull(val))
		b.WriteString(";")
	}
	sum := sha256.Sum256([]byte(b.String()))
	return sum[:]
}

// splitDumpByTable streams INSERT rows and writes per-table JSONL files.
// This mirrors the Java pipeline and keeps later comparisons table-local.
func splitDumpByTable(dumpFile string, columnsMap map[string][]string, pkMap map[string][]string, tempDir string, label string, p *mpb.Progress, progressLabel string) (map[string]string, error) {
	insertParser := parser.NewInsertParser()
	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)
	tablePaths := make(map[string]string)
	missingTables := make(map[string]bool)
	seenTables := make(map[string]bool)

	err := insertParser.ParseInsertsStream(dumpFile, columnsMap, p, progressLabel, func(row *parser.InsertRow) {
		seenTables[row.Table] = true
		resolved, ok := resolveTableName(pkMap, row.Table)
		if !ok {
			missingTables[row.Table] = true
			return
		}
		bw, ok := writers[resolved]
		if !ok {
			name := sanitizeFilename(resolved)
			path := filepath.Join(tempDir, fmt.Sprintf("%s_%s.jsonl", label, name))
			f, e := os.Create(path)
			if e != nil {
				return
			}
			files[resolved] = f
			bw = bufio.NewWriter(f)
			writers[resolved] = bw
			tablePaths[resolved] = path
		}
		jsonStr, e := row.ToJSON()
		if e != nil {
			return
		}
		bw.WriteString(jsonStr)
		bw.WriteByte('\n')
	})

	for _, bw := range writers {
		bw.Flush()
	}
	for _, f := range files {
		f.Close()
	}

	if err != nil {
		return nil, err
	}
	if len(missingTables) > 0 {
		names := make([]string, 0, len(missingTables))
		for name := range missingTables {
			names = append(names, name)
		}
		sort.Strings(names)
		logger.Debug("splitDumpByTable: %s missing PK tables (%d): %s", label, len(names), strings.Join(names, ", "))
	}
	logger.Debug("splitDumpByTable: %s saw %d tables in INSERTs, wrote %d table files", label, len(seenTables), len(tablePaths))
	return tablePaths, nil
}

// sanitizeFilename converts a table name into a safe filename and adds
// a short hash suffix to avoid collisions.
func sanitizeFilename(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		b.WriteString("table")
	}
	sum := sha256.Sum256([]byte(name))
	return fmt.Sprintf("%s_%s", b.String(), hex.EncodeToString(sum[:4]))
}

// resolveTableName maps a table name from INSERTs to the canonical PK map key.
// This handles case-only differences between CREATE TABLE and INSERT sections.
func resolveTableName(pkMap map[string][]string, name string) (string, bool) {
	if _, ok := pkMap[name]; ok {
		return name, true
	}
	lower := strings.ToLower(name)
	for key := range pkMap {
		if strings.ToLower(key) == lower {
			return key, true
		}
	}
	return "", false
}

// loadTableJSONL reads a JSONL file (one row per line) and stores it in SQLite.
func loadTableJSONL(db *sql.DB, path string, pkCols []string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	tx, stmt, err := beginSQLiteInsert(db)
	if err != nil {
		return fmt.Errorf("begin insert: %w", err)
	}
	defer stmt.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	batchCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		row, err := parser.FromJSON(line)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("parse json: %w", err)
		}
		hash := hashPK(row, pkCols)
		rh := rowHash(row)
		if _, e := stmt.Exec(hash, rh, line); e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert row: %w", e)
		}
		batchCount++
		if batchCount >= sqliteBatchSize {
			if err := stmt.Close(); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("close stmt: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit: %w", err)
			}
			tx, stmt, err = beginSQLiteInsert(db)
			if err != nil {
				return fmt.Errorf("begin insert: %w", err)
			}
			batchCount = 0
		}
	}
	if err := scanner.Err(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("scan jsonl: %w", err)
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// compareNewRows streams the new JSONL file and produces inserts/updates.
func compareNewRows(db *sql.DB, newFilePath string, pkCols []string, result *ComparisonResult, seen map[string]struct{}) error {
	f, err := os.Open(newFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	getStmt, err := db.Prepare(`SELECT row_hash, row_json FROM rows WHERE pk_hash = ?`)
	if err != nil {
		return fmt.Errorf("prepare lookup: %w", err)
	}
	defer getStmt.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		newRow, err := parser.FromJSON(line)
		if err != nil {
			return fmt.Errorf("parse json: %w", err)
		}
		hash := hashPK(newRow, pkCols)
		newHash := rowHash(newRow)

		var oldHash []byte
		var oldRowStr string
		err = getStmt.QueryRow(hash).Scan(&oldHash, &oldRowStr)
		if err == sql.ErrNoRows {
			result.Changes += fmt.Sprintf("-- NEW RECORD IN %s\n", newRow.Table)
			result.Changes += buildInsertStatement(newRow)
			result.Changes += "\n\n"
			result.InsertCount++
			continue
		} else if err != nil {
			return fmt.Errorf("lookup: %w", err)
		}

		seen[hash] = struct{}{}
		if bytes.Equal(oldHash, newHash) {
			continue
		}

		oldRow, err := parser.FromJSON(oldRowStr)
		if err != nil {
			return fmt.Errorf("parse old json: %w", err)
		}
		updates := findUpdates(oldRow, newRow)
		if len(updates) > 0 {
			result.Changes += fmt.Sprintf("-- TABLE %s\n", newRow.Table)
			for col, oldVal := range updates {
				result.Changes += fmt.Sprintf("-- %s old value: %s\n", col, oldVal)
			}
			result.Changes += buildUpdateStatement(newRow.Table, newRow, pkCols, updates)
			result.Changes += "\n\n"
			result.UpdateCount++
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan new jsonl: %w", err)
	}
	return nil
}

// emitDeletionsFromDB scans old rows and emits deletions for unseen PKs.
func emitDeletionsFromDB(db *sql.DB, pkCols []string, seen map[string]struct{}, result *ComparisonResult) error {
	rows, err := db.Query(`SELECT pk_hash, row_json FROM rows`)
	if err != nil {
		return fmt.Errorf("query rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hash, rowStr string
		if err := rows.Scan(&hash, &rowStr); err != nil {
			return fmt.Errorf("scan deletion: %w", err)
		}
		if _, ok := seen[hash]; ok {
			continue
		}
		oldRow, err := parser.FromJSON(rowStr)
		if err != nil {
			return fmt.Errorf("decode deletion row: %w", err)
		}
		result.Deletions += fmt.Sprintf("-- DELETED FROM %s: %s\n", oldRow.Table, hash)
		result.Deletions += buildDeleteStatement(oldRow.Table, oldRow, pkCols)
		result.Deletions += "\n\n"
		result.DeleteCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}
	return nil
}

// applySQLitePragmas configures SQLite for fast, temp-file workloads.
// These settings prioritize throughput over durability.
func applySQLitePragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=OFF;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA temp_store=MEMORY;",
		fmt.Sprintf("PRAGMA cache_size=%d;", -sqliteCacheKB),
		"PRAGMA page_size=32768;",
		fmt.Sprintf("PRAGMA mmap_size=%d;", sqliteMmapMB<<20),
		"PRAGMA busy_timeout=5000;",
	}
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("apply pragma %q: %w", pragma, err)
		}
	}
	return nil
}

// setupSQLiteSchema creates the per-table rows table.
// Each table DB stores only one table's rows.
func setupSQLiteSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS rows (
			pk_hash TEXT PRIMARY KEY,
			row_hash BLOB NOT NULL,
			row_json TEXT NOT NULL
		);`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

// beginSQLiteInsert starts a transaction and returns an insert statement.
// We batch inserts for speed and commit every sqliteBatchSize rows.
func beginSQLiteInsert(db *sql.DB) (*sql.Tx, *sql.Stmt, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO rows(pk_hash, row_hash, row_json) VALUES(?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	return tx, stmt, nil
}

func insertRowsToSQLite(db *sql.DB, insertParser *parser.InsertParser, filename string, columns map[string][]string, pkMap map[string][]string, p *mpb.Progress) error {
	tx, stmt, err := beginSQLiteInsert(db)
	if err != nil {
		return fmt.Errorf("begin insert: %w", err)
	}
	defer stmt.Close()

	batchCount := 0
	var firstErr error

	err = insertParser.ParseInsertsStream(filename, columns, p, "", func(row *parser.InsertRow) {
		if firstErr != nil {
			return
		}
		pkCols, hasPK := pkMap[row.Table]
		if !hasPK {
			return
		}
		hash := hashPK(row, pkCols)
		val, mErr := json.Marshal(row)
		if mErr != nil {
			firstErr = mErr
			return
		}
		rh := rowHash(row)
		if _, e := stmt.Exec(hash, rh, string(val)); e != nil {
			firstErr = e
			return
		}
		batchCount++
		if batchCount >= sqliteBatchSize {
			if e := stmt.Close(); e != nil && firstErr == nil {
				firstErr = e
				return
			}
			if e := tx.Commit(); e != nil && firstErr == nil {
				firstErr = e
				return
			}
			nextTx, nextStmt, nextErr := beginSQLiteInsert(db)
			if nextErr != nil {
				firstErr = nextErr
				return
			}
			tx = nextTx
			stmt = nextStmt
			batchCount = 0
		}
	})
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("parse inserts: %w", err)
	}
	if firstErr != nil {
		_ = tx.Rollback()
		return fmt.Errorf("insert rows: %w", firstErr)
	}
	if err := stmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close insert stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit insert: %w", err)
	}
	return nil
}

// compareTableSQLite compares a single table:
// 1) load old rows into SQLite
// 2) stream new rows and emit inserts/updates
// 3) emit deletions for unseen PKs
func compareTableSQLite(table string, pkCols []string, oldFilePath, newFilePath string) (*ComparisonResult, *TableTiming, error) {
	result := &ComparisonResult{TableName: table}
	if oldFilePath == "" && newFilePath == "" {
		return result, &TableTiming{Table: table}, nil
	}

	timing := &TableTiming{Table: table}

	dbFile, err := os.CreateTemp("", "sqldumpdiff_table_*.db")
	if err != nil {
		return nil, fmt.Errorf("create temp db: %w", err)
	}
	dbPath := dbFile.Name()
	dbFile.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + "-shm")
	defer os.Remove(dbPath + "-wal")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		return nil, err
	}
	if err := setupSQLiteSchema(db); err != nil {
		return nil, err
	}

	// Load old rows into SQLite
	if oldFilePath != "" {
		start := time.Now()
		if err := loadTableJSONL(db, oldFilePath, pkCols); err != nil {
			return nil, nil, err
		}
		timing.LoadMS = time.Since(start).Milliseconds()
		if timingEnabled || logger.IsDebugEnabled() {
			logger.Debug("Timing: %s load old rows took %s", table, time.Since(start))
		}
	}

	seen := make(map[string]struct{})

	// Process new rows
	if newFilePath != "" {
		start := time.Now()
		if err := compareNewRows(db, newFilePath, pkCols, result, seen); err != nil {
			return nil, nil, err
		}
		timing.CompMS = time.Since(start).Milliseconds()
		if timingEnabled || logger.IsDebugEnabled() {
			logger.Debug("Timing: %s compare new rows took %s", table, time.Since(start))
		}
	}

	// Deletions
	start := time.Now()
	if err := emitDeletionsFromDB(db, pkCols, seen, result); err != nil {
		return nil, nil, err
	}
	timing.DelMS = time.Since(start).Milliseconds()
	if timingEnabled || logger.IsDebugEnabled() {
		logger.Debug("Timing: %s deletions step took %s", table, time.Since(start))
	}

	return result, timing, nil
}

// ConfigureSQLiteTunables allows CLI to override defaults.
// ConfigureSQLiteTunables allows the CLI to override defaults before processing.
func ConfigureSQLiteTunables(cacheKB, mmapMB, batchSize, workers int) {
	if cacheKB > 0 {
		sqliteCacheKB = cacheKB
	}
	if mmapMB >= 0 {
		sqliteMmapMB = mmapMB
	}
	if batchSize > 0 {
		sqliteBatchSize = batchSize
	}
	if workers >= 0 {
		sqliteWorkers = workers
	}
}

// ConfigureTiming enables timing diagnostics independent of debug logging.
func ConfigureTiming(enabled bool) {
	timingEnabled = enabled
}

// DeltaGenerator generates delta SQL between two dumps
type DeltaGenerator struct {
	pkMap         map[string][]string
	oldColumnsMap map[string][]string
	newColumnsMap map[string][]string
}

// NewDeltaGenerator creates a new delta generator
func NewDeltaGenerator(pkMap, oldColumnsMap, newColumnsMap map[string][]string) *DeltaGenerator {
	return &DeltaGenerator{
		pkMap:         pkMap,
		oldColumnsMap: oldColumnsMap,
		newColumnsMap: newColumnsMap,
	}
}

// GenerateDelta is the high-level pipeline:
// 1) split dumps into per-table JSONL files
// 2) compare tables in parallel using per-table SQLite DBs
// 3) stream SQL output and return summary counts
func (dg *DeltaGenerator) GenerateDelta(oldFile, newFile string, p *mpb.Progress, out io.Writer) (Summary, *TimingMeta, error) {
	logger.Debug("GenerateDelta: Starting delta generation")
	tmpDir, err := os.MkdirTemp("", "sqldumpdiff-sqlite-*")
	if err != nil {
		return Summary{}, nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	summary := Summary{}
	timingMeta := &TimingMeta{}

	// Write header
	fmt.Fprintln(out, "-- Full Delta Update Script")
	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 0;")
	fmt.Fprintln(out)

	// Split dumps into per-table JSONL files
	logger.Debug("GenerateDelta: Splitting dumps by table")
	splitStart := time.Now()
	oldTableFiles := make(map[string]string)
	newTableFiles := make(map[string]string)
	var splitErr error
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		m, err := splitDumpByTable(oldFile, dg.oldColumnsMap, dg.pkMap, tmpDir, "old", p, "Parsing old dump")
		if err != nil {
			splitErr = err
			return
		}
		mu.Lock()
		oldTableFiles = m
		mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		m, err := splitDumpByTable(newFile, dg.newColumnsMap, dg.pkMap, tmpDir, "new", p, "Parsing new dump")
		if err != nil {
			splitErr = err
			return
		}
		mu.Lock()
		newTableFiles = m
		mu.Unlock()
	}()
	wg.Wait()
	if splitErr != nil {
		return Summary{}, nil, splitErr
	}
	if timingEnabled || logger.IsDebugEnabled() {
		logger.Debug("Timing: split dumps took %s", time.Since(splitStart))
	}
	timingMeta.SplitMS = time.Since(splitStart).Milliseconds()

	// Compare per table in parallel
	allTables := make(map[string]bool)
	for t := range oldTableFiles {
		allTables[t] = true
	}
	for t := range newTableFiles {
		allTables[t] = true
	}
	logger.Debug("GenerateDelta: old tables=%d new tables=%d all=%d pkMap=%d", len(oldTableFiles), len(newTableFiles), len(allTables), len(dg.pkMap))

	logger.Debug("GenerateDelta: Comparing %d tables", len(allTables))
	compareStart := time.Now()
	var outMu sync.Mutex
	var sumMu sync.Mutex
	var writeMS int64
	var writeFormatMS int64
	var writeIOMS int64

	maxWorkers := runtime.NumCPU()
	if sqliteWorkers > 0 {
		maxWorkers = sqliteWorkers
	}
	sem := make(chan struct{}, maxWorkers)

	var compareBar *mpb.Bar
	if p != nil && len(allTables) > 0 {
		compareBar = p.New(
			int64(len(allTables)),
			mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding(" ").Rbound("]"),
			mpb.PrependDecorators(
				decor.Name("Comparing tables", decor.WC{W: 20, C: decor.DindentRight | decor.DextraSpace}),
				decor.CountersNoUnit("%d / %d", decor.WC{W: 18, C: decor.DindentRight | decor.DextraSpace}),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 5}),
			),
		)
	}

	compareWg := sync.WaitGroup{}
	var timingMu sync.Mutex
	timings := make([]*TableTiming, 0, len(allTables))
	for table := range allTables {
		pkCols, hasPK := dg.pkMap[table]
		if !hasPK {
			continue
		}
		oldPath := oldTableFiles[table]
		newPath := newTableFiles[table]
		compareWg.Add(1)
		go func(tbl string, pk []string, oldFilePath, newFilePath string) {
			defer compareWg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res, timing, err := compareTableSQLite(tbl, pk, oldFilePath, newFilePath)
			if err != nil {
				logger.Error("GenerateDelta: Table %s compare error: %v", tbl, err)
				return
			}
			if timing != nil {
				timingMu.Lock()
				timings = append(timings, timing)
				timingMu.Unlock()
			}
			if res.Changes != "" || res.Deletions != "" {
				formatStart := time.Now()
				var b strings.Builder
				if res.Changes != "" {
					b.WriteString(res.Changes)
				}
				if res.Deletions != "" {
					b.WriteString("-- DELETIONS\n")
					b.WriteString(res.Deletions)
				}
				formatDur := time.Since(formatStart).Milliseconds()
				atomic.AddInt64(&writeFormatMS, formatDur)

				outMu.Lock()
				ioStart := time.Now()
				fmt.Fprint(out, b.String())
				outMu.Unlock()
				ioDur := time.Since(ioStart).Milliseconds()
				atomic.AddInt64(&writeIOMS, ioDur)
				atomic.AddInt64(&writeMS, formatDur+ioDur)
			}
			sumMu.Lock()
			summary.InsertCount += res.InsertCount
			summary.UpdateCount += res.UpdateCount
			summary.DeleteCount += res.DeleteCount
			sumMu.Unlock()
			if compareBar != nil {
				compareBar.IncrBy(1)
			}
		}(table, pkCols, oldPath, newPath)
	}
	compareWg.Wait()
	if compareBar != nil {
		compareBar.SetTotal(int64(len(allTables)), true)
	}
	if timingEnabled || logger.IsDebugEnabled() {
		logger.Debug("Timing: compare tables took %s", time.Since(compareStart))
	}
	timingMeta.CompareMS = time.Since(compareStart).Milliseconds()

	// Log top 10 slowest tables when debug logging is enabled.
	if (timingEnabled || logger.IsDebugEnabled()) && len(timings) > 0 {
		sort.Slice(timings, func(i, j int) bool {
			return timings[i].TotalMS() > timings[j].TotalMS()
		})
		limit := 10
		if len(timings) < limit {
			limit = len(timings)
		}
		logger.Debug("Timing: top %d slowest tables (ms):", limit)
		for i := 0; i < limit; i++ {
			t := timings[i]
			logger.Debug("  %s total=%d load=%d compare=%d delete=%d", t.Table, t.TotalMS(), t.LoadMS, t.CompMS, t.DelMS)
		}
	}
	var deleteMS int64
	for _, t := range timings {
		deleteMS += t.DelMS
	}
	timingMeta.DeleteMS = deleteMS
	timingMeta.WriteMS = atomic.LoadInt64(&writeMS)
	timingMeta.WriteFormatMS = atomic.LoadInt64(&writeFormatMS)
	timingMeta.WriteIOMS = atomic.LoadInt64(&writeIOMS)

	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 1;")
	logger.Debug("GenerateDelta: Completed")
	timingMeta.Tables = timings
	return summary, timingMeta, nil
}

func getColumnValueCaseInsensitive(row *parser.InsertRow, colName string) string {
	// First try exact match
	if val, exists := row.Data[colName]; exists {
		return val
	}
	// Try case-insensitive match
	colNameLower := strings.ToLower(colName)
	for key, val := range row.Data {
		if strings.ToLower(key) == colNameLower {
			return val
		}
	}
	return ""
}

func hashPK(row *parser.InsertRow, pkCols []string) string {
	var parts []string
	for _, col := range pkCols {
		val := getColumnValueCaseInsensitive(row, col)
		// Normalize the PK value (strip quotes, handle NULL)
		parts = append(parts, normalizeNull(val))
	}
	data := strings.Join(parts, "|")
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func findUpdates(oldRow, newRow *parser.InsertRow) map[string]string {
	updates := make(map[string]string)
	for _, col := range newRow.Columns {
		newVal := getColumnValueCaseInsensitive(newRow, col)
		oldVal := getColumnValueCaseInsensitive(oldRow, col)
		oldNorm := normalizeNull(oldVal)
		newNorm := normalizeNull(newVal)
		if oldNorm != newNorm {
			updates[col] = oldVal
			logger.Debug("findUpdates: Column %s changed: '%s' -> '%s' (normalized: '%s' -> '%s')", col, oldVal, newVal, oldNorm, newNorm)
		}
	}
	return updates
}

func normalizeNull(val string) string {
	if strings.ToUpper(val) == "NULL" {
		return "\x00"
	}
	if val == "" {
		return ""
	}

	// Strip surrounding single quotes (matching Java's behavior)
	val = strings.TrimSpace(val)
	if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
		val = val[1 : len(val)-1]
		// Unescape doubled single quotes: '' -> '
		val = strings.ReplaceAll(val, "''", "'")
	}

	return val
}

func buildInsertStatement(row *parser.InsertRow) string {
	var cols []string
	var vals []string

	for _, col := range row.Columns {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		vals = append(vals, formatSQLValue(row.Data[col]))
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);",
		row.Table,
		strings.Join(cols, ", "),
		strings.Join(vals, ", "))
}

func buildUpdateStatement(table string, row *parser.InsertRow, pkCols []string, changedCols map[string]string) string {
	var setParts []string
	for col := range changedCols {
		val := row.Data[col]
		setParts = append(setParts, fmt.Sprintf("`%s`=%s", col, formatSQLValue(val)))
	}

	whereClause := buildWhereClause(row, pkCols)

	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s;",
		table,
		strings.Join(setParts, ", "),
		whereClause)
}

func buildDeleteStatement(table string, row *parser.InsertRow, pkCols []string) string {
	whereClause := buildWhereClause(row, pkCols)
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s;", table, whereClause)
}

func buildWhereClause(row *parser.InsertRow, pkCols []string) string {
	var parts []string
	for _, col := range pkCols {
		val := row.Data[col]
		normalized := normalizeNull(val)
		if normalized == "\x00" {
			parts = append(parts, fmt.Sprintf("`%s` IS NULL", col))
		} else {
			// Re-escape quotes for SQL
			escaped := strings.ReplaceAll(normalized, "'", "''")
			parts = append(parts, fmt.Sprintf("`%s`='%s'", col, escaped))
		}
	}
	return strings.Join(parts, " AND ")
}

// formatSQLValue formats a value for SQL output
// Handles normalized values (quotes removed) and NULL
func formatSQLValue(val string) string {
	normalized := normalizeNull(val)
	if normalized == "\x00" {
		return "NULL"
	}
	// Escape single quotes for SQL
	escaped := strings.ReplaceAll(normalized, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}
