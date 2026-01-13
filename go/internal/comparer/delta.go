package comparer

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
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

func applySQLitePragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA cache_size=-200000;",
		"PRAGMA locking_mode=NORMAL;",
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
		`CREATE TABLE IF NOT EXISTS old_rows (
			table_name TEXT NOT NULL,
			pk_hash TEXT NOT NULL,
			row_hash BLOB NOT NULL,
			row_json TEXT NOT NULL,
			PRIMARY KEY (table_name, pk_hash)
		);`,
		`CREATE TABLE IF NOT EXISTS new_rows (
			table_name TEXT NOT NULL,
			pk_hash TEXT NOT NULL,
			row_hash BLOB NOT NULL,
			row_json TEXT NOT NULL,
			PRIMARY KEY (table_name, pk_hash)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_old_rows_pk ON old_rows(table_name, pk_hash);`,
		`CREATE INDEX IF NOT EXISTS idx_new_rows_pk ON new_rows(table_name, pk_hash);`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
	}
	return nil
}

func beginSQLiteInsert(db *sql.DB, table string) (*sql.Tx, *sql.Stmt, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, nil, err
	}
	stmt, err := tx.Prepare(fmt.Sprintf(`INSERT OR REPLACE INTO %s(table_name, pk_hash, row_hash, row_json) VALUES(?, ?, ?, ?)`, table))
	if err != nil {
		_ = tx.Rollback()
		return nil, nil, err
	}
	return tx, stmt, nil
}

func insertRowsToSQLite(db *sql.DB, table string, insertParser *parser.InsertParser, filename string, columns map[string][]string, pkMap map[string][]string, p *mpb.Progress) error {
	tx, stmt, err := beginSQLiteInsert(db, table)
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
		rh := rowHash(row)
		if _, e := stmt.Exec(row.Table, hash, rh, string(val)); e != nil {
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
			nextTx, nextStmt, nextErr := beginSQLiteInsert(db, table)
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

type rowItem struct {
	tableName string
	pkHash    string
	rowHash   []byte
	rowJSON   string
	destTable string
}

func ingestParallelToSQLite(db *sql.DB, insertParser *parser.InsertParser, oldFile, newFile string, oldCols, newCols map[string][]string, pkMap map[string][]string, p *mpb.Progress) error {
	items := make(chan rowItem, 4096)
	errCh := make(chan error, 2)

	parseFile := func(filename string, columns map[string][]string, destTable string) {
		err := insertParser.ParseInsertsStream(filename, columns, p, func(row *parser.InsertRow) {
			pkCols, hasPK := pkMap[row.Table]
			if !hasPK {
				return
			}
			hash := hashPK(row, pkCols)
			val, mErr := json.Marshal(row)
			if mErr != nil {
				errCh <- mErr
				return
			}
			rh := rowHash(row)
			items <- rowItem{
				tableName: row.Table,
				pkHash:    hash,
				rowHash:   rh,
				rowJSON:   string(val),
				destTable: destTable,
			}
		})
		errCh <- err
	}

	go parseFile(oldFile, oldCols, "old_rows")
	go parseFile(newFile, newCols, "new_rows")

	tx, oldStmt, err := beginSQLiteInsert(db, "old_rows")
	if err != nil {
		return fmt.Errorf("begin insert old: %w", err)
	}
	newTx := tx
	newStmt := oldStmt
	if oldStmt != nil {
		// reuse tx but different stmt for new_rows
		newStmt, err = tx.Prepare(`INSERT OR REPLACE INTO new_rows(table_name, pk_hash, row_hash, row_json) VALUES(?, ?, ?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("prepare insert new: %w", err)
		}
	}

	defer oldStmt.Close()
	defer newStmt.Close()

	doneParsers := 0
	batchCount := 0
	for doneParsers < 2 {
		select {
		case err := <-errCh:
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			doneParsers++
		case item := <-items:
			stmt := oldStmt
			if item.destTable == "new_rows" {
				stmt = newStmt
			}
			if _, e := stmt.Exec(item.tableName, item.pkHash, item.rowHash, item.rowJSON); e != nil {
				_ = tx.Rollback()
				return fmt.Errorf("insert rows: %w", e)
			}
			batchCount++
			if batchCount >= sqliteBatchSize {
				if err := oldStmt.Close(); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("close stmt: %w", err)
				}
				if err := newStmt.Close(); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("close stmt: %w", err)
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit insert: %w", err)
				}
				tx, oldStmt, err = beginSQLiteInsert(db, "old_rows")
				if err != nil {
					return fmt.Errorf("begin insert old: %w", err)
				}
				newTx = tx
				_ = newTx
				newStmt, err = tx.Prepare(`INSERT OR REPLACE INTO new_rows(table_name, pk_hash, row_hash, row_json) VALUES(?, ?, ?, ?)`)
				if err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("prepare insert new: %w", err)
				}
				batchCount = 0
			}
		}
	}

	// drain any remaining items
	for {
		select {
		case item := <-items:
			stmt := oldStmt
			if item.destTable == "new_rows" {
				stmt = newStmt
			}
			if _, e := stmt.Exec(item.tableName, item.pkHash, item.rowHash, item.rowJSON); e != nil {
				_ = tx.Rollback()
				return fmt.Errorf("insert rows: %w", e)
			}
		default:
			goto done
		}
	}
done:
	if err := oldStmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close stmt: %w", err)
	}
	if err := newStmt.Close(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("close stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit insert: %w", err)
	}
	return nil
}

func compareSQLiteTables(db *sql.DB, pkMap map[string][]string, out io.Writer, summary *Summary) error {
	// Inserts
	insRows, err := db.Query(`
		SELECT n.table_name, n.pk_hash, n.row_json
		FROM new_rows n
		LEFT JOIN old_rows o
		ON o.table_name = n.table_name AND o.pk_hash = n.pk_hash
		WHERE o.pk_hash IS NULL
	`)
	if err != nil {
		return fmt.Errorf("insert query: %w", err)
	}
	for insRows.Next() {
		var table, hash, newRowStr string
		if err := insRows.Scan(&table, &hash, &newRowStr); err != nil {
			insRows.Close()
			return fmt.Errorf("scan insert: %w", err)
		}
		_, hasPK := pkMap[table]
		if !hasPK {
			continue
		}
		var newRow parser.InsertRow
		if err := json.Unmarshal([]byte(newRowStr), &newRow); err != nil {
			insRows.Close()
			return fmt.Errorf("decode insert row: %w", err)
		}
		fmt.Fprintf(out, "-- NEW RECORD IN %s\n", table)
		fmt.Fprint(out, buildInsertStatement(&newRow))
		fmt.Fprint(out, "\n\n")
		summary.InsertCount++
	}
	if err := insRows.Err(); err != nil {
		insRows.Close()
		return fmt.Errorf("iterate inserts: %w", err)
	}
	insRows.Close()

	// Updates (hash diff)
	upRows, err := db.Query(`
		SELECT n.table_name, n.pk_hash, n.row_json, o.row_json
		FROM new_rows n
		JOIN old_rows o
		ON o.table_name = n.table_name AND o.pk_hash = n.pk_hash
		WHERE n.row_hash <> o.row_hash
	`)
	if err != nil {
		return fmt.Errorf("update query: %w", err)
	}
	defer upRows.Close()

	for upRows.Next() {
		var table, hash, newRowStr, oldRowStr string
		if err := upRows.Scan(&table, &hash, &newRowStr, &oldRowStr); err != nil {
			return fmt.Errorf("scan update: %w", err)
		}
		pkCols, hasPK := pkMap[table]
		if !hasPK {
			continue
		}
		var newRow parser.InsertRow
		if err := json.Unmarshal([]byte(newRowStr), &newRow); err != nil {
			return fmt.Errorf("decode new row: %w", err)
		}
		var oldRow parser.InsertRow
		if err := json.Unmarshal([]byte(oldRowStr), &oldRow); err != nil {
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
		_ = hash
	}
	if err := upRows.Err(); err != nil {
		return fmt.Errorf("iterate updates: %w", err)
	}
	return nil
}

func emitDeletionsSQL(db *sql.DB, pkMap map[string][]string, out io.Writer, summary *Summary) error {
	rows, err := db.Query(`
		SELECT o.table_name, o.pk_hash, o.row_json
		FROM old_rows o
		LEFT JOIN new_rows n
		ON n.table_name = o.table_name AND n.pk_hash = o.pk_hash
		WHERE n.pk_hash IS NULL
	`)
	if err != nil {
		return fmt.Errorf("query deletions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var table, hash, rowStr string
		if err := rows.Scan(&table, &hash, &rowStr); err != nil {
			return fmt.Errorf("scan deletion: %w", err)
		}
		pkCols, hasPK := pkMap[table]
		if !hasPK {
			continue
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

	dbPath := tmpDir + string(os.PathSeparator) + "sqldumpdiff.db"
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()
	if err := applySQLitePragmas(db); err != nil {
		return Summary{}, err
	}
	if err := setupSQLiteSchema(db); err != nil {
		return Summary{}, err
	}

	summary := Summary{}

	// Write header
	fmt.Fprintln(out, "-- Full Delta Update Script")
	fmt.Fprintln(out, "SET FOREIGN_KEY_CHECKS = 0;")
	fmt.Fprintln(out)

	// Index old/new rows in parallel parsing, single writer to avoid SQLITE_BUSY
	logger.Debug("GenerateDelta: Indexing old/new files (parallel parse, single writer)")
	if err := ingestParallelToSQLite(db, insertParser, oldFile, newFile, dg.oldColumnsMap, dg.newColumnsMap, dg.pkMap, p); err != nil {
		return Summary{}, err
	}

	// Compare using set-based joins
	logger.Debug("GenerateDelta: Comparing rows")
	if err := compareSQLiteTables(db, dg.pkMap, out, &summary); err != nil {
		return Summary{}, err
	}

	// Emit deletions
	logger.Debug("GenerateDelta: Scanning for deletions")
	fmt.Fprintln(out, "-- DELETIONS")
	if err := emitDeletionsSQL(db, dg.pkMap, out, &summary); err != nil {
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
