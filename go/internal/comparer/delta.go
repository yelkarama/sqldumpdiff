package comparer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/younes/sqldumpdiff/internal/logger"
	"github.com/younes/sqldumpdiff/internal/parser"
	"github.com/younes/sqldumpdiff/internal/store"
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

// GenerateDelta compares old and new dumps and generates delta
func (dg *DeltaGenerator) GenerateDelta(oldFile, newFile string, p *mpb.Progress) ([]*ComparisonResult, error) {
	logger.Debug("GenerateDelta: Starting delta generation")
	insertParser := parser.NewInsertParser()

	// Parse old and new files in parallel
	type parseResult struct {
		rows map[string][]*parser.InsertRow
		err  error
	}

	oldCh := make(chan parseResult, 1)
	newCh := make(chan parseResult, 1)

	logger.Debug("GenerateDelta: Parsing old file (parallel): %s", oldFile)
	go func() {
		rows, err := insertParser.ParseInserts(oldFile, dg.oldColumnsMap, p)
		oldCh <- parseResult{rows: rows, err: err}
	}()

	logger.Debug("GenerateDelta: Parsing new file (parallel): %s", newFile)
	go func() {
		rows, err := insertParser.ParseInserts(newFile, dg.newColumnsMap, p)
		newCh <- parseResult{rows: rows, err: err}
	}()

	// Wait for both results
	oldRes := <-oldCh
	if oldRes.err != nil {
		logger.Error("GenerateDelta: Error parsing old file: %v", oldRes.err)
		return nil, fmt.Errorf("parsing old file: %w", oldRes.err)
	}
	oldRows := oldRes.rows
	logger.Debug("GenerateDelta: Old file has %d tables", len(oldRows))

	newRes := <-newCh
	if newRes.err != nil {
		logger.Error("GenerateDelta: Error parsing new file: %v", newRes.err)
		return nil, fmt.Errorf("parsing new file: %w", newRes.err)
	}
	newRows := newRes.rows
	logger.Debug("GenerateDelta: New file has %d tables", len(newRows))

	// Get all tables
	allTables := make(map[string]bool)
	for table := range oldRows {
		allTables[table] = true
	}
	for table := range newRows {
		allTables[table] = true
	}
	logger.Debug("GenerateDelta: Found %d tables total to compare", len(allTables))

	// Compare each table
	var results []*ComparisonResult
	tablesCompared := 0

	var bar *mpb.Bar
	if p != nil {
		bar = p.New(
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

	for table := range allTables {
		pkCols, hasPK := dg.pkMap[table]
		if !hasPK {
			logger.Debug("GenerateDelta: Skipping table %s - no PRIMARY KEY defined", table)
			if bar != nil {
				bar.IncrBy(1)
			}
			continue
		}

		logger.Debug("GenerateDelta: Comparing table %s with PK columns: %v", table, pkCols)
		result := dg.compareTable(table, pkCols, oldRows[table], newRows[table])
		results = append(results, result)
		logger.Debug("GenerateDelta: Table %s comparison done - %d inserts, %d updates, %d deletes", table, result.InsertCount, result.UpdateCount, result.DeleteCount)
		tablesCompared++
		if bar != nil {
			bar.IncrBy(1)
		}
	}
	if bar != nil {
		bar.SetTotal(int64(len(allTables)), true)
	}

	// Sort results by table name
	sort.Slice(results, func(i, j int) bool {
		return results[i].TableName < results[j].TableName
	})

	logger.Debug("GenerateDelta: Compared %d tables, total results: %d", tablesCompared, len(results))
	return results, nil
}

func (dg *DeltaGenerator) compareTable(table string, pkCols []string, oldRows, newRows []*parser.InsertRow) *ComparisonResult {
	result := &ComparisonResult{
		TableName: table,
	}

	// Build old rows index
	oldStore := store.NewMemoryStore()
	for _, row := range oldRows {
		pkHash := hashPK(row, pkCols)
		oldStore.Set(pkHash, row)
	}

	matched := make(map[string]bool)
	var changes strings.Builder
	var deletions strings.Builder

	// Process new rows
	for _, newRow := range newRows {
		pkHash := hashPK(newRow, pkCols)
		oldRow := oldStore.Get(pkHash)

		if oldRow == nil {
			// New record
			changes.WriteString(fmt.Sprintf("-- NEW RECORD IN %s\n", table))
			changes.WriteString(buildInsertStatement(newRow))
			changes.WriteString("\n\n")
			result.InsertCount++
		} else {
			matched[pkHash] = true

			// Check for updates
			updates := findUpdates(oldRow, newRow)
			if len(updates) > 0 {
				for col, oldVal := range updates {
					changes.WriteString(fmt.Sprintf("-- %s old value: %s\n", col, oldVal))
				}
				changes.WriteString(buildUpdateStatement(table, newRow, pkCols, updates))
				changes.WriteString("\n\n")
				result.UpdateCount++
			}
		}
	}

	// Find deletions
	for pkHash, oldRow := range oldStore.GetAll() {
		if !matched[pkHash] {
			deletions.WriteString(fmt.Sprintf("-- DELETED FROM %s: %s\n", table, pkHash))
			deletions.WriteString(buildDeleteStatement(table, oldRow, pkCols))
			deletions.WriteString("\n\n")
			result.DeleteCount++
		}
	}

	result.Changes = changes.String()
	result.Deletions = deletions.String()

	return result
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
