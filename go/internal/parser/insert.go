package parser

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/younes/sqldumpdiff/internal/logger"
)

// InsertRow represents a parsed INSERT row with table, columns, and data map.
type InsertRow struct {
	Table   string
	Columns []string
	Data    map[string]string
}

// InsertParser handles parsing of INSERT statements from dumps.
type InsertParser struct {
}

// NewInsertParser creates a new insert parser
func NewInsertParser() *InsertParser {
	return &InsertParser{}
}

// InsertAccumulator helps build complete INSERT statements across multiple lines.
// It tracks quote/paren state to know when a statement is complete.
type InsertAccumulator struct {
	buffer              strings.Builder
	inSingleQuote       bool
	inDoubleQuote       bool
	escapeNext          bool
	parenDepth          int
	inInsert            bool
	statementsProcessed int
}

// ProcessLine processes a line and returns any complete INSERT statements found.
func (acc *InsertAccumulator) ProcessLine(line string) []string {
	var results []string

	// Check if starting a new INSERT
	if !acc.inInsert && strings.Contains(strings.ToUpper(line), "INSERT INTO") {
		acc.buffer.Reset()
		acc.buffer.WriteString(line)
		acc.buffer.WriteString("\n")
		acc.inInsert = true
		acc.inSingleQuote = false
		acc.inDoubleQuote = false
		acc.escapeNext = false
		acc.parenDepth = 0

		acc.processLineContent(line)

		if strings.HasSuffix(strings.TrimSpace(line), ";") && acc.parenDepth == 0 {
			results = append(results, acc.buffer.String())
			acc.buffer.Reset()
			acc.inInsert = false
			acc.statementsProcessed++
		}
		return results
	}

	// Continue processing if in INSERT
	if acc.inInsert {
		acc.buffer.WriteString(line)
		acc.buffer.WriteString("\n")
		acc.processLineContent(line)

		if strings.HasSuffix(strings.TrimSpace(line), ";") && acc.parenDepth == 0 {
			results = append(results, acc.buffer.String())
			acc.buffer.Reset()
			acc.inInsert = false
			acc.statementsProcessed++
		}
	}

	return results
}

// processLineContent tracks quote state and parenthesis depth while scanning a line.
func (acc *InsertAccumulator) processLineContent(line string) {
	for _, c := range line {
		if acc.escapeNext {
			acc.escapeNext = false
			continue
		}
		if c == '\\' {
			acc.escapeNext = true
			continue
		}
		if c == '\'' && !acc.inDoubleQuote {
			acc.inSingleQuote = !acc.inSingleQuote
		} else if c == '"' && !acc.inSingleQuote {
			acc.inDoubleQuote = !acc.inDoubleQuote
		} else if !acc.inSingleQuote && !acc.inDoubleQuote {
			if c == '(' {
				acc.parenDepth++
			} else if c == ')' {
				acc.parenDepth--
			}
		}
	}
}

// Finalize returns any incomplete statement at EOF.
func (acc *InsertAccumulator) Finalize() []string {
	var results []string
	if acc.inInsert && acc.buffer.Len() > 0 {
		results = append(results, acc.buffer.String())
		acc.statementsProcessed++
	}
	return results
}

// ParseInsertsStream reads INSERT statements from a file and calls onRow for each row.
// This is the streaming entrypoint used by the comparer for large files.
func (ip *InsertParser) ParseInsertsStream(filename string, columns map[string][]string, p *mpb.Progress, label string, onRow func(*InsertRow)) error {
	logger.Debug("ParseInsertsStream: Opening file %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		logger.Error("ParseInsertsStream: Failed to open file %s: %v", filename, err)
		return err
	}
	defer file.Close()

	// Get file size for progress bar
	fi, err := file.Stat()
	if err != nil {
		logger.Error("ParseInsertsStream: Failed to stat file: %v", err)
		return err
	}
	fileSize := fi.Size()
	logger.Debug("ParseInsertsStream: File size: %d bytes", fileSize)

	var bar *mpb.Bar
	if p != nil {
		desc := label
		if desc == "" {
			desc = fmt.Sprintf("Parsing %s", filepath.Base(filename))
		}
		bar = p.New(
			fileSize,
			mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding(" ").Rbound("]"),
			mpb.PrependDecorators(
				decor.Name(desc, decor.WC{W: 20, C: decor.DindentRight | decor.DextraSpace}),
				decor.CountersKibiByte("% .2f / % .2f", decor.WC{W: 18, C: decor.DindentRight | decor.DextraSpace}),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 5}),
			),
		)
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 16MB buffer for large lines

	acc := &InsertAccumulator{}
	bytesRead := int64(0)
	for scanner.Scan() {
		line := scanner.Text()
		lineSize := int64(len(line)) + 1
		bytesRead += lineSize
		if bar != nil {
			bar.IncrBy(int(lineSize))
		}

		// Process the line and get any complete INSERT statements
		statements := acc.ProcessLine(line)
		for _, stmt := range statements {
			rows, err := ip.ExpandInsert(stmt, columns)
			if err != nil {
				logger.Debug("ParseInsertsStream: Failed to expand INSERT statement: %v", err)
				continue
			}

			for _, row := range rows {
				onRow(row)
			}
		}
	}

	// Handle any incomplete statement at EOF
	statements := acc.Finalize()
	for _, stmt := range statements {
		rows, err := ip.ExpandInsert(stmt, columns)
		if err != nil {
			logger.Debug("ParseInsertsStream: Failed to expand INSERT statement: %v", err)
			continue
		}

		for _, row := range rows {
			onRow(row)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("ParseInsertsStream: Scanner error: %v", err)
		return err
	}
	if bar != nil {
		bar.SetTotal(bytesRead, true)
	}

	logger.Debug("ParseInsertsStream: Processed %d INSERT statements", acc.statementsProcessed)
	return nil
}

// ParseInserts reads INSERT statements and returns an in-memory map of rows.
// This is used for smaller workflows; large-file flows use ParseInsertsStream.
func (ip *InsertParser) ParseInserts(filename string, columns map[string][]string, p *mpb.Progress) (map[string][]*InsertRow, error) {
	logger.Debug("ParseInserts: Opening file %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		logger.Error("ParseInserts: Failed to open file %s: %v", filename, err)
		return nil, err
	}
	defer file.Close()

	// Get file size for progress bar
	fi, err := file.Stat()
	if err != nil {
		logger.Error("ParseInserts: Failed to stat file: %v", err)
		return nil, err
	}
	fileSize := fi.Size()
	logger.Debug("ParseInserts: File size: %d bytes", fileSize)

	var bar *mpb.Bar
	if p != nil {
		bar = p.New(
			fileSize,
			mpb.BarStyle().Lbound("[").Filler("█").Tip("█").Padding(" ").Rbound("]"),
			mpb.PrependDecorators(
				decor.Name(fmt.Sprintf("Parsing %s", filepath.Base(filename)), decor.WC{W: 20, C: decor.DindentRight | decor.DextraSpace}),
				decor.CountersKibiByte("% .2f / % .2f", decor.WC{W: 18, C: decor.DindentRight | decor.DextraSpace}),
			),
			mpb.AppendDecorators(
				decor.Percentage(decor.WC{W: 5}),
			),
		)
	}

	tableRows := make(map[string][]*InsertRow)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // 16MB buffer for large lines

	acc := &InsertAccumulator{}
	rowsExtracted := 0
	bytesRead := int64(0)
	for scanner.Scan() {
		line := scanner.Text()
		lineSize := int64(len(line)) + 1
		bytesRead += lineSize // +1 for newline
		if bar != nil {
			bar.IncrBy(int(lineSize))
		}

		// Process the line and get any complete INSERT statements
		statements := acc.ProcessLine(line)
		for _, stmt := range statements {
			rows, err := ip.ExpandInsert(stmt, columns)
			if err != nil {
				logger.Debug("ParseInserts: Failed to expand INSERT statement: %v", err)
				continue
			}

			for _, row := range rows {
				tableRows[row.Table] = append(tableRows[row.Table], row)
				rowsExtracted++
			}
		}
	}

	// Handle any incomplete statement at EOF
	statements := acc.Finalize()
	for _, stmt := range statements {
		rows, err := ip.ExpandInsert(stmt, columns)
		if err != nil {
			logger.Debug("ParseInserts: Failed to expand INSERT statement: %v", err)
			continue
		}

		for _, row := range rows {
			tableRows[row.Table] = append(tableRows[row.Table], row)
			rowsExtracted++
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("ParseInserts: Scanner error: %v", err)
		return nil, err
	}
	if bar != nil {
		bar.SetTotal(bytesRead, true)
	}

	logger.Debug("ParseInserts: Processed %d INSERT statements, extracted %d rows across %d tables", acc.statementsProcessed, rowsExtracted, len(tableRows))
	return tableRows, nil
}

// ExpandInsert parses a single INSERT statement into multiple rows.
func (ip *InsertParser) ExpandInsert(insertStmt string, columnsMap map[string][]string) ([]*InsertRow, error) {
	// Normalize whitespace
	normalized := strings.Join(strings.Fields(insertStmt), " ")
	normalized = strings.TrimSpace(normalized)

	// Extract table name
	upperStmt := strings.ToUpper(normalized)
	insertIdx := strings.Index(upperStmt, "INSERT INTO")
	if insertIdx == -1 {
		return nil, fmt.Errorf("not an INSERT statement")
	}

	// Find table name after backtick
	restOfStmt := normalized[insertIdx+11:] // Skip "INSERT INTO"
	restOfStmt = strings.TrimSpace(restOfStmt)

	var table string
	if strings.HasPrefix(restOfStmt, "`") {
		endIdx := strings.Index(restOfStmt[1:], "`")
		if endIdx == -1 {
			return nil, fmt.Errorf("invalid table name")
		}
		table = restOfStmt[1 : endIdx+1]
		restOfStmt = restOfStmt[endIdx+2:]
	} else {
		// Table name without backticks
		parts := strings.FieldsFunc(restOfStmt, func(r rune) bool { return r == '(' || r == ' ' })
		if len(parts) == 0 {
			return nil, fmt.Errorf("invalid table name")
		}
		table = parts[0]
	}

	logger.Debug("ExpandInsert: Parsing INSERT for table: %s", table)

	// Extract columns and values
	var columns []string

	// Check if columns are explicit
	restOfStmt = strings.TrimSpace(restOfStmt)
	if strings.HasPrefix(restOfStmt, "(") {
		// Find matching closing paren for columns
		depth := 0
		var endIdx int
		for i, c := range restOfStmt {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth == 0 {
					endIdx = i
					break
				}
			}
		}
		if endIdx == 0 {
			return nil, fmt.Errorf("invalid column list")
		}

		colStr := restOfStmt[1:endIdx]
		for _, col := range strings.Split(colStr, ",") {
			col = strings.TrimSpace(col)
			col = strings.Trim(col, "`'\"")
			if col != "" {
				columns = append(columns, col)
			}
		}

		restOfStmt = restOfStmt[endIdx+1:]
	}

	// Get values section
	restOfStmt = strings.TrimSpace(restOfStmt)
	valuesIdx := strings.ToUpper(restOfStmt)
	valuesPos := strings.Index(valuesIdx, "VALUES")
	if valuesPos == -1 {
		return nil, fmt.Errorf("no VALUES clause found")
	}

	valuesPart := restOfStmt[valuesPos+6:] // Skip "VALUES"
	valuesPart = strings.TrimSpace(valuesPart)

	// Remove trailing semicolon
	if strings.HasSuffix(valuesPart, ";") {
		valuesPart = valuesPart[:len(valuesPart)-1]
	}
	valuesPart = strings.TrimSpace(valuesPart)

	// If no explicit columns, use from schema
	if len(columns) == 0 {
		if columnsMap != nil && len(columnsMap[table]) > 0 {
			columns = columnsMap[table]
		}
	}

	if len(columns) == 0 {
		logger.Debug("ExpandInsert: No columns defined for table %s", table)
		return nil, fmt.Errorf("no columns defined for table %s", table)
	}

	// Split value groups
	groups := splitValueGroupsWithQuotes(valuesPart)
	logger.Debug("ExpandInsert: Table %s has %d value groups", table, len(groups))

	var rows []*InsertRow
	for _, group := range groups {
		group = strings.TrimSpace(group)
		if strings.HasPrefix(group, "(") && strings.HasSuffix(group, ")") {
			group = group[1 : len(group)-1]
		}

		values := parseValuesWithQuotes(group)
		if len(values) != len(columns) {
			logger.Debug("ExpandInsert: Skipping row with mismatched column count (expected %d, got %d)", len(columns), len(values))
			logger.Debug("ExpandInsert: Columns: %v", columns)
			logger.Debug("ExpandInsert: Values sample: %v", values[:min(5, len(values))])
			logger.Debug("ExpandInsert: Group snippet: %.200s...", group)
			continue
		}

		data := make(map[string]string)
		for i, col := range columns {
			data[col] = values[i]
		}

		rows = append(rows, &InsertRow{
			Table:   table,
			Columns: columns,
			Data:    data,
		})
	}

	logger.Debug("ExpandInsert: Table %s created %d rows", table, len(rows))
	return rows, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// splitValueGroupsWithQuotes splits value groups while respecting quote boundaries
func splitValueGroupsWithQuotes(valuesPart string) []string {
	var groups []string
	var buf strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	escapeNext := false
	parenDepth := 0

	for _, c := range valuesPart {
		if escapeNext {
			buf.WriteRune(c)
			escapeNext = false
			continue
		}
		if c == '\\' {
			buf.WriteRune(c)
			escapeNext = true
			continue
		}
		if c == '\'' && !inDoubleQuote {
			buf.WriteRune(c)
			inSingleQuote = !inSingleQuote
			continue
		}
		if c == '"' && !inSingleQuote {
			buf.WriteRune(c)
			inDoubleQuote = !inDoubleQuote
			continue
		}
		if !inSingleQuote && !inDoubleQuote {
			if c == '(' {
				parenDepth++
				buf.WriteRune(c)
				continue
			}
			if c == ')' {
				parenDepth--
				buf.WriteRune(c)
				if parenDepth == 0 {
					groups = append(groups, buf.String())
					buf.Reset()
				}
				continue
			}
			if c == ',' && parenDepth == 0 {
				continue // Skip separator between groups
			}
		}
		buf.WriteRune(c)
	}

	if buf.Len() > 0 {
		remaining := strings.TrimSpace(buf.String())
		if remaining != "" {
			groups = append(groups, remaining)
		}
	}

	return groups
}

// parseValuesWithQuotes parses individual values while respecting quote boundaries
// Keeps quotes in values to match Java parser behavior for proper comparison
func parseValuesWithQuotes(valuesStr string) []string {
	var values []string
	var buf strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	escapeNext := false
	depth := 0 // Track parenthesis depth for nested structures

	for _, c := range valuesStr {
		if escapeNext {
			buf.WriteRune(c)
			escapeNext = false
			continue
		}
		if c == '\\' {
			buf.WriteRune(c)
			escapeNext = true
			continue
		}
		if c == '\'' && !inDoubleQuote && depth == 0 {
			buf.WriteRune(c) // Include the quote in the value (unlike previous version)
			inSingleQuote = !inSingleQuote
			continue
		}
		if c == '"' && !inSingleQuote && depth == 0 {
			buf.WriteRune(c) // Include the quote in the value
			inDoubleQuote = !inDoubleQuote
			continue
		}

		// Track parenthesis for nested structures
		if !inSingleQuote && !inDoubleQuote {
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
			}
		}

		if !inSingleQuote && !inDoubleQuote && depth == 0 && c == ',' {
			val := strings.TrimSpace(buf.String())
			if val == "NULL" {
				values = append(values, "NULL")
			} else {
				values = append(values, val)
			}
			buf.Reset()
		} else {
			buf.WriteRune(c)
		}
	}

	// Last value
	val := strings.TrimSpace(buf.String())
	if val == "NULL" {
		values = append(values, "NULL")
	} else {
		values = append(values, val)
	}

	return values
}

// ToJSON converts InsertRow to JSON for storage
func (ir *InsertRow) ToJSON() (string, error) {
	b, err := json.Marshal(ir)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// FromJSON parses JSON back into InsertRow
func FromJSON(jsonStr string) (*InsertRow, error) {
	var row InsertRow
	if err := json.Unmarshal([]byte(jsonStr), &row); err != nil {
		return nil, err
	}
	return &row, nil
}
