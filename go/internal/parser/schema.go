package parser

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/schollz/progressbar/v3"
	"github.com/younes/sqldumpdiff/internal/logger"
)

// SchemaParser handles parsing of SQL schemas to extract table information
type SchemaParser struct {
	createTableRegex *regexp.Regexp
	primaryKeyRegex  *regexp.Regexp
	columnRegex      *regexp.Regexp
}

// NewSchemaParser creates a new schema parser
func NewSchemaParser() *SchemaParser {
	// Use simple case-insensitive matching without regex flags since Go's regexp has limited support
	return &SchemaParser{
		createTableRegex: regexp.MustCompile(`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`" + `?([^` + "`" + `\s(]+)` + "`" + `?`),
		primaryKeyRegex:  regexp.MustCompile(`PRIMARY\s+KEY\s*\(\s*` + "`" + `?([^` + "`" + `)]+)` + "`" + `?\s*\)`),
		columnRegex:      regexp.MustCompile("`" + "([^`]+)" + "`"),
	}
}

// ParseSchemas extracts primary key information from CREATE TABLE statements
func (sp *SchemaParser) ParseSchemas(filename string) (map[string][]string, error) {
	logger.Debug("ParseSchemas: Opening file %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		logger.Error("ParseSchemas: Failed to open file %s: %v", filename, err)
		return nil, err
	}
	defer file.Close()

	// Get file size for progress bar
	fi, err := file.Stat()
	if err != nil {
		logger.Error("ParseSchemas: Failed to stat file: %v", err)
		return nil, err
	}
	fileSize := fi.Size()

	// Create progress bar
	bar := progressbar.NewOptions64(
		fileSize,
		progressbar.OptionSetDescription(fmt.Sprintf("Schema %s", filename)),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowCount(),
	)
	defer bar.Close()

	pkMap := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	// Increase buffer to handle very long lines in large CREATE statements
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024) // 16MB max token
	var currentTable string
	var inCreateTable bool
	var tableBuffer strings.Builder
	tablesProcessed := 0

	for scanner.Scan() {
		line := scanner.Text()
		bar.Add64(int64(len(line)) + 1) // +1 for newline

		if sp.createTableRegex.MatchString(strings.ToUpper(line)) {
			matches := sp.createTableRegex.FindStringSubmatch(strings.ToUpper(line))
			if len(matches) > 1 {
				// Extract table name from original line (preserve case)
				baseRegex := regexp.MustCompile(`CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`" + `?([^` + "`" + `\s(]+)` + "`" + `?`)
				baseMatches := baseRegex.FindStringSubmatch(strings.ToUpper(line))
				if len(baseMatches) > 1 {
					// Get the actual case-sensitive table name
					indexInUpper := strings.Index(strings.ToUpper(line), strings.ToUpper(baseMatches[1]))
					if indexInUpper >= 0 {
						currentTable = line[indexInUpper : indexInUpper+len(baseMatches[1])]
						inCreateTable = true
						tableBuffer.Reset()
						tableBuffer.WriteString(line)
						tableBuffer.WriteString("\n")
						logger.Debug("ParseSchemas: Found CREATE TABLE statement for table: %s", currentTable)
					}
				}
			}
		} else if inCreateTable {
			tableBuffer.WriteString(line)
			tableBuffer.WriteString("\n")

			// Check for end of CREATE TABLE - mysqldump format ends with ") ENGINE=..." or just ");"
			trimmedLine := strings.TrimSpace(line)
			if strings.HasSuffix(trimmedLine, ";") {
				// End of CREATE TABLE
				tableDef := tableBuffer.String()
				tableDefUppercase := strings.ToUpper(tableDef)

				if sp.primaryKeyRegex.MatchString(tableDefUppercase) {
					pkMatches := sp.primaryKeyRegex.FindStringSubmatch(tableDefUppercase)
					if len(pkMatches) > 1 {
						pkStr := pkMatches[1]
						// pkStr could be "ID" or "ID,NAME" - split by comma
						pkParts := strings.Split(pkStr, ",")
						var pkColumns []string
						for _, part := range pkParts {
							col := strings.TrimSpace(part)
							// Remove backticks if present
							col = strings.Trim(col, "`")
							// Convert to lowercase to match data map keys
							col = strings.ToLower(col)
							if col != "" {
								pkColumns = append(pkColumns, col)
							}
						}
						if len(pkColumns) > 0 {
							pkMap[currentTable] = pkColumns
							logger.Debug("ParseSchemas: Table %s has PRIMARY KEY columns: %v", currentTable, pkColumns)
						}
					}
				}

				tablesProcessed++
				inCreateTable = false
				currentTable = ""
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("ParseSchemas: Scanner error: %v", err)
		return nil, err
	}

	logger.Debug("ParseSchemas: Processed %d tables, found %d with primary keys", tablesProcessed, len(pkMap))
	return pkMap, nil
}

// ParseColumns extracts column names from CREATE TABLE statements
func (sp *SchemaParser) ParseColumns(filename string) (map[string][]string, error) {
	logger.Debug("ParseColumns: Opening file %s", filename)
	file, err := os.Open(filename)
	if err != nil {
		logger.Error("ParseColumns: Failed to open file %s: %v", filename, err)
		return nil, err
	}
	defer file.Close()

	// Get file size for progress bar
	fi, err := file.Stat()
	if err != nil {
		logger.Error("ParseColumns: Failed to stat file: %v", err)
		return nil, err
	}
	fileSize := fi.Size()

	// Create progress bar
	bar := progressbar.NewOptions64(
		fileSize,
		progressbar.OptionSetDescription(fmt.Sprintf("Columns %s", filename)),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowCount(),
	)
	defer bar.Close()

	columnsMap := make(map[string][]string)
	scanner := bufio.NewScanner(file)
	// Increase buffer to handle very long lines in large CREATE statements
	scanner.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024) // 16MB max token
	var currentTable string
	var inCreateTable bool
	var tableBuffer strings.Builder
	tablesProcessed := 0

	columnLineRegex := regexp.MustCompile("`" + `(\w+)` + "`" + `\s+(\w+)`)

	for scanner.Scan() {
		line := scanner.Text()
		bar.Add64(int64(len(line)) + 1) // +1 for newline

		if sp.createTableRegex.MatchString(line) {
			matches := sp.createTableRegex.FindStringSubmatch(line)
			if len(matches) > 1 {
				currentTable = matches[1]
				inCreateTable = true
				tableBuffer.Reset()
				tableBuffer.WriteString(line)
				tableBuffer.WriteString("\n")
				logger.Debug("ParseColumns: Found CREATE TABLE statement for table: %s", currentTable)
			}
		} else if inCreateTable {
			tableBuffer.WriteString(line)
			tableBuffer.WriteString("\n")

			trimmedLine := strings.TrimSpace(line)
			if strings.HasSuffix(trimmedLine, ";") {
				// Extract columns
				tableDef := tableBuffer.String()
				lines := strings.Split(tableDef, "\n")
				var columns []string

				for _, l := range lines {
					// Skip non-column lines
					trimmedL := strings.TrimSpace(l)

					// Skip CREATE TABLE line, KEY definitions, CONSTRAINT, closing parenthesis
					if strings.Contains(l, "CREATE TABLE") ||
						strings.HasPrefix(trimmedL, "PRIMARY KEY") ||
						strings.HasPrefix(trimmedL, "KEY ") ||
						strings.HasPrefix(trimmedL, "UNIQUE KEY") ||
						strings.HasPrefix(trimmedL, "CONSTRAINT") ||
						strings.HasPrefix(trimmedL, "FULLTEXT") ||
						strings.HasPrefix(trimmedL, ")") ||
						trimmedL == "" {
						continue
					}

					// Only match lines that start with backtick (column definitions)
					if strings.HasPrefix(trimmedL, "`") && columnLineRegex.MatchString(l) {
						matches := columnLineRegex.FindStringSubmatch(l)
						if len(matches) > 1 {
							columns = append(columns, matches[1])
						}
					}
				}

				if len(columns) > 0 {
					columnsMap[currentTable] = columns
					logger.Debug("ParseColumns: Table %s has %d columns: %v", currentTable, len(columns), columns)
				}

				tablesProcessed++
				inCreateTable = false
				currentTable = ""
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("ParseColumns: Scanner error: %v", err)
		return nil, err
	}

	logger.Debug("ParseColumns: Processed %d tables, extracted columns from %d", tablesProcessed, len(columnsMap))
	return columnsMap, nil
}
