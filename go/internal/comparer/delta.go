package comparer

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/vbauerster/mpb/v8"
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

const sqliteBatchSize = 10000

func applySQLitePragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=OFF;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA cache_size=-200000;",
		"PRAGMA locking_mode=EXCLUSIVE;",
		"PRAGMA busy_timeout=20000;",
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
			table_name TEXT NOT NULL,
			pk_hash TEXT NOT NULL,
			row_json TEXT NOT NULL,
			PRIMARY KEY (table_name, pk_hash)
		);`,
		`CREATE TABLE IF NOT EXISTS seen (
			table_name TEXT NOT NULL,
			pk_hash TEXT NOT NULL,
			PRIMARY KEY (table_name, pk_hash)
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
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO rows(table_name, pk_hash, row_json) VALUES(?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	return tx, stmt, nil
}

func beginSQLiteSeenInsert(db *sql.DB) (*sql.Tx, *sql.Stmt, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO seen(table_name, pk_hash) VALUES(?, ?)`)
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

	err = insertParser.ParseInsertsStream(filename, columns, p, func(row *parser.InsertRow) {
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
		if _, e := stmt.Exec(row.Table, hash, string(val)); e != nil {
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

func compareSQLiteDBs(dbOld, dbNew, dbSeen *sql.DB, pkMap map[string][]string, out io.Writer, summary *Summary) error {
	getStmt, err := dbOld.Prepare(`SELECT row_json FROM rows WHERE table_name = ? AND pk_hash = ?`)
	if err != nil {
		return fmt.Errorf("prepare lookup: %w", err)
	}
	defer getStmt.Close()

	tx, seenStmt, err := beginSQLiteSeenInsert(dbSeen)
	if err != nil {
		return fmt.Errorf("begin seen insert: %w", err)
	}
	defer seenStmt.Close()

	rows, err := dbNew.Query(`SELECT table_name, pk_hash, row_json FROM rows`)
	if err != nil {
		return fmt.Errorf("scan new rows: %w", err)
	}
	defer rows.Close()

	batchCount := 0
	for rows.Next() {
		var table, hash, rowStr string
		if err := rows.Scan(&table, &hash, &rowStr); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("scan new row: %w", err)
		}
		pkCols, hasPK := pkMap[table]
		if !hasPK {
			continue
		}
		var newRow parser.InsertRow
		if err := json.Unmarshal([]byte(rowStr), &newRow); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("decode new row: %w", err)
		}

		var oldRow parser.InsertRow
		oldStr := ""
		scanErr := getStmt.QueryRow(table, hash).Scan(&oldStr)
		if scanErr == sql.ErrNoRows {
			fmt.Fprintf(out, "-- NEW RECORD IN %s\n", table)
			fmt.Fprint(out, buildInsertStatement(&newRow))
			fmt.Fprint(out, "\n\n")
			summary.InsertCount++
		} else if scanErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("lookup old row: %w", scanErr)
		} else {
			if err := json.Unmarshal([]byte(oldStr), &oldRow); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("decode old row: %w", err)
			}
			updates := findUpdates(&oldRow, &newRow)
			if len(updates) > 0 {
				for col, oldVal := range updates {
					fmt.Fprintf(out, "-- %s old value: %s\n", col, oldVal)
				}
				fmt.Fprint(out, buildUpdateStatement(table, &newRow, pkCols, updates))
				fmt.Fprint(out, "\n\n")
				summary.UpdateCount++
			}
		}

		if _, e := seenStmt.Exec(table, hash); e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert seen: %w", e)
		}
		batchCount++
		if batchCount >= sqliteBatchSize {
			if e := seenStmt.Close(); e != nil {
				_ = tx.Rollback()
				return fmt.Errorf("close seen stmt: %w", e)
			}
			if e := tx.Commit(); e != nil {
				return fmt.Errorf("commit seen: %w", e)
			}
			nextTx, nextStmt, nextErr := beginSQLiteSeenInsert(dbSeen)
			if nextErr != nil {
				return fmt.Errorf("begin seen insert: %w", nextErr)
			}
			tx = nextTx
			seenStmt = nextStmt
			batchCount = 0
		}
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("iterate new rows: %w", err)
	}
	if err := seenStmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close seen stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit seen: %w", err)
	}
	return nil
}

func emitDeletions(dbOld, dbSeen *sql.DB, pkMap map[string][]string, out io.Writer, summary *Summary) error {
	rows, err := dbOld.Query(`SELECT table_name, pk_hash, row_json FROM rows`)
	if err != nil {
		return fmt.Errorf("query deletions: %w", err)
	}
	defer rows.Close()

	seenStmt, err := dbSeen.Prepare(`SELECT 1 FROM seen WHERE table_name = ? AND pk_hash = ?`)
	if err != nil {
		return fmt.Errorf("prepare seen lookup: %w", err)
	}
	defer seenStmt.Close()

	for rows.Next() {
		var table, hash, rowStr string
		if err := rows.Scan(&table, &hash, &rowStr); err != nil {
			return fmt.Errorf("scan deletion: %w", err)
		}
		pkCols, hasPK := pkMap[table]
		if !hasPK {
			continue
		}
		var exists int
		if err := seenStmt.QueryRow(table, hash).Scan(&exists); err == nil {
			continue
		} else if err != sql.ErrNoRows {
			return fmt.Errorf("lookup seen: %w", err)
		}

		var oldRow parser.InsertRow
		if err := json.Unmarshal([]byte(rowStr), &oldRow); err != nil {
			return fmt.Errorf("decode deletion row: %w", err)
		}
		fmt.Fprintf(out, "-- DELETED FROM %s: %s\n", table, hash)
		fmt.Fprint(out, buildDeleteStatement(table, &oldRow, pkCols))
		fmt.Fprint(out, "\n\n")
		summary.DeleteCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate deletions: %w", err)
	}
	return nil
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
	insertParser := parser.NewInsertParser()

	tmpDir, err := os.MkdirTemp("", "sqldumpdiff-sqlite-*")
	if err != nil {
		return Summary{}, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	oldPath := tmpDir + string(os.PathSeparator) + "old.db"
	newPath := tmpDir + string(os.PathSeparator) + "new.db"
	seenPath := tmpDir + string(os.PathSeparator) + "seen.db"

	dbOld, err := sql.Open("sqlite", oldPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open sqlite: %w", err)
	}
	defer dbOld.Close()
	dbOld.SetMaxOpenConns(1)

	dbNew, err := sql.Open("sqlite", newPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open sqlite (new): %w", err)
	}
	defer dbNew.Close()
	dbNew.SetMaxOpenConns(1)

	dbSeen, err := sql.Open("sqlite", seenPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open sqlite (seen): %w", err)
	}
	defer dbSeen.Close()
	dbSeen.SetMaxOpenConns(1)

	if err := applySQLitePragmas(dbOld); err != nil {
		return Summary{}, err
	}
	if err := applySQLitePragmas(dbNew); err != nil {
		return Summary{}, err
	}
	if err := applySQLitePragmas(dbSeen); err != nil {
		return Summary{}, err
	}
	if err := setupSQLiteSchema(dbOld); err != nil {
		return Summary{}, err
	}
	if err := setupSQLiteSchema(dbNew); err != nil {
		return Summary{}, err
	}
	if err := setupSQLiteSchema(dbSeen); err != nil {
		return Summary{}, err
	}

	summary := Summary{}

	// Write header
	fmt.Fprintln(out, "-- Full Delta Update Script")
	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 0;")
	fmt.Fprintln(out)

	// Index old/new rows in parallel
	logger.Debug("GenerateDelta: Indexing old/new files in parallel")
	oldCh := make(chan error, 1)
	newCh := make(chan error, 1)
	go func() {
		oldCh <- insertRowsToSQLite(dbOld, insertParser, oldFile, dg.oldColumnsMap, dg.pkMap, p)
	}()
	go func() {
		newCh <- insertRowsToSQLite(dbNew, insertParser, newFile, dg.newColumnsMap, dg.pkMap, p)
	}()
	if err := <-oldCh; err != nil {
		return Summary{}, err
	}
	if err := <-newCh; err != nil {
		return Summary{}, err
	}

	// Compare using new DB and old DB
	logger.Debug("GenerateDelta: Comparing rows")
	if err := compareSQLiteDBs(dbOld, dbNew, dbSeen, dg.pkMap, out, &summary); err != nil {
		return Summary{}, err
	}

	// Emit deletions
	logger.Debug("GenerateDelta: Scanning for deletions")
	fmt.Fprintln(out, "-- DELETIONS")
	if err := emitDeletions(dbOld, dbSeen, dg.pkMap, out, &summary); err != nil {
		return Summary{}, err
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
