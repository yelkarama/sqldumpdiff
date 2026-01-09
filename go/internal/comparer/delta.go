package comparer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

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
	pkMap          map[string][]string
	oldColumnsMap  map[string][]string
	newColumnsMap  map[string][]string
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
func (dg *DeltaGenerator) GenerateDelta(oldFile, newFile string) ([]*ComparisonResult, error) {
	logger.Debug("GenerateDelta: Starting delta generation")
	insertParser := parser.NewInsertParser()

	// Parse old and new files
	logger.Debug("GenerateDelta: Parsing old file: %s", oldFile)
	oldRows, err := insertParser.ParseInserts(oldFile, dg.oldColumnsMap)
	if err != nil {
		logger.Error("GenerateDelta: Error parsing old file: %v", err)
		return nil, fmt.Errorf("parsing old file: %w", err)
	}
	logger.Debug("GenerateDelta: Old file has %d tables", len(oldRows))

	logger.Debug("GenerateDelta: Parsing new file: %s", newFile)
	newRows, err := insertParser.ParseInserts(newFile, dg.newColumnsMap)
	if err != nil {
		logger.Error("GenerateDelta: Error parsing new file: %v", err)
		return nil, fmt.Errorf("parsing new file: %w", err)
	}
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
	for table := range allTables {
		pkCols, hasPK := dg.pkMap[table]
		if !hasPK {
			logger.Debug("GenerateDelta: Skipping table %s - no PRIMARY KEY defined", table)
			continue
		}

		logger.Debug("GenerateDelta: Comparing table %s with PK columns: %v", table, pkCols)
		result := dg.compareTable(table, pkCols, oldRows[table], newRows[table])
		results = append(results, result)
		logger.Debug("GenerateDelta: Table %s comparison done - %d inserts, %d updates, %d deletes", table, result.InsertCount, result.UpdateCount, result.DeleteCount)
		tablesCompared++
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
				changes.WriteString(buildUpdateStatement(table, newRow, pkCols))
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

func hashPK(row *parser.InsertRow, pkCols []string) string {
	var parts []string
	for _, col := range pkCols {
		val := row.Data[col]
		parts = append(parts, val)
	}
	data := strings.Join(parts, "|")
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func findUpdates(oldRow, newRow *parser.InsertRow) map[string]string {
	updates := make(map[string]string)
	for col, newVal := range newRow.Data {
		oldVal := oldRow.Data[col]
		if normalizeNull(oldVal) != normalizeNull(newVal) {
			updates[col] = oldVal
		}
	}
	return updates
}

func normalizeNull(val string) string {
	if val == "" || strings.ToUpper(val) == "NULL" {
		return ""
	}
	return val
}

func buildInsertStatement(row *parser.InsertRow) string {
	var cols []string
	var vals []string

	for _, col := range row.Columns {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		val := row.Data[col]
		if val == "" {
			vals = append(vals, "NULL")
		} else {
			vals = append(vals, fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''")))
		}
	}

	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s);",
		row.Table,
		strings.Join(cols, ", "),
		strings.Join(vals, ", "))
}

func buildUpdateStatement(table string, row *parser.InsertRow, pkCols []string) string {
	var setParts []string
	for _, col := range row.Columns {
		isPK := false
		for _, pk := range pkCols {
			if col == pk {
				isPK = true
				break
			}
		}
		if isPK {
			continue
		}

		val := row.Data[col]
		if val == "" {
			setParts = append(setParts, fmt.Sprintf("`%s`=NULL", col))
		} else {
			setParts = append(setParts, fmt.Sprintf("`%s`='%s'", col, strings.ReplaceAll(val, "'", "''")))
		}
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
		if val == "" {
			parts = append(parts, fmt.Sprintf("`%s` IS NULL", col))
		} else {
			parts = append(parts, fmt.Sprintf("`%s`='%s'", col, strings.ReplaceAll(val, "'", "''")))
		}
	}
	return strings.Join(parts, " AND ")
}
