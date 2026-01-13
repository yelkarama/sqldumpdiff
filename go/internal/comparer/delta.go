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

// Summary holds aggregate counts for delta generation.
type Summary struct {
	InsertCount int
	UpdateCount int
	DeleteCount int
}

var sqliteBatchSize = 20000
var sqliteCacheKB = 800000
var sqliteMmapMB = 128
var sqliteWorkers = 0

func rowHash(row *parser.InsertRow) []byte {
	keys := make([]string, 0, len(row.Data))
	for k := range row.Data {
		keys = append(keys, strings.ToLower(k))
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(normalizeNull(row.Data[k]))
		b.WriteString(";")
	}
	sum := sha256.Sum256([]byte(b.String()))
	return sum[:]
}

func splitDumpByTable(dumpFile string, columnsMap map[string][]string, pkMap map[string][]string, tempDir string, label string, p *mpb.Progress, progressLabel string) (map[string]string, error) {
	insertParser := parser.NewInsertParser()
	writers := make(map[string]*bufio.Writer)
	files := make(map[string]*os.File)
	tablePaths := make(map[string]string)

	err := insertParser.ParseInsertsStream(dumpFile, columnsMap, p, progressLabel, func(row *parser.InsertRow) {
		if _, ok := pkMap[row.Table]; !ok {
			return
		}
		bw, ok := writers[row.Table]
		if !ok {
			name := sanitizeFilename(row.Table)
			path := filepath.Join(tempDir, fmt.Sprintf("%s_%s.jsonl", label, name))
			f, e := os.Create(path)
			if e != nil {
				return
			}
			files[row.Table] = f
			bw = bufio.NewWriter(f)
			writers[row.Table] = bw
			tablePaths[row.Table] = path
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
	return tablePaths, nil
}

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
		return "table"
	}
	return b.String()
}

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

func compareTableSQLite(table string, pkCols []string, oldFilePath, newFilePath string) (*ComparisonResult, error) {
	result := &ComparisonResult{TableName: table}
	if oldFilePath == "" && newFilePath == "" {
		return result, nil
	}

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
		if err := loadTableJSONL(db, oldFilePath, pkCols); err != nil {
			return nil, err
		}
	}

	seen := make(map[string]struct{})

	// Process new rows
	if newFilePath != "" {
		if err := compareNewRows(db, newFilePath, pkCols, result, seen); err != nil {
			return nil, err
		}
	}

	// Deletions
	if err := emitDeletionsFromDB(db, pkCols, seen, result); err != nil {
		return nil, err
	}

	return result, nil
}

// ConfigureSQLiteTunables allows CLI to override defaults.
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

// GenerateDelta compares old and new dumps and generates delta to out.
func (dg *DeltaGenerator) GenerateDelta(oldFile, newFile string, p *mpb.Progress, out io.Writer) (Summary, error) {
	logger.Debug("GenerateDelta: Starting delta generation")
	tmpDir, err := os.MkdirTemp("", "sqldumpdiff-sqlite-*")
	if err != nil {
		return Summary{}, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	summary := Summary{}

	// Write header
	fmt.Fprintln(out, "-- Full Delta Update Script")
	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 0;")
	fmt.Fprintln(out)

	// Split dumps into per-table JSONL files
	logger.Debug("GenerateDelta: Splitting dumps by table")
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
		return Summary{}, splitErr
	}

	// Compare per table in parallel
	allTables := make(map[string]bool)
	for t := range oldTableFiles {
		allTables[t] = true
	}
	for t := range newTableFiles {
		allTables[t] = true
	}

	logger.Debug("GenerateDelta: Comparing %d tables", len(allTables))
	var outMu sync.Mutex
	var sumMu sync.Mutex

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
			res, err := compareTableSQLite(tbl, pk, oldFilePath, newFilePath)
			if err != nil {
				logger.Error("GenerateDelta: Table %s compare error: %v", tbl, err)
				return
			}
			if res.Changes != "" || res.Deletions != "" {
				outMu.Lock()
				if res.Changes != "" {
					fmt.Fprint(out, res.Changes)
				}
				if res.Deletions != "" {
					fmt.Fprint(out, "-- DELETIONS\n")
					fmt.Fprint(out, res.Deletions)
				}
				outMu.Unlock()
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

	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 1;")
	logger.Debug("GenerateDelta: Completed")
	return summary, nil
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
	for col, newVal := range newRow.Data {
		oldVal := oldRow.Data[col]
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
	if val == "" || strings.ToUpper(val) == "NULL" {
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
		if normalized == "" {
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
	if normalized == "" {
		return "NULL"
	}
	// Escape single quotes for SQL
	escaped := strings.ReplaceAll(normalized, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}
