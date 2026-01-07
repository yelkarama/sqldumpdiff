package com.sqldumpdiff;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.StreamSupport;

/**
 * Parses INSERT statements from SQL dump files.
 * Handles multi-line and multi-row INSERT statements.
 */
public class InsertParser {

    private static final Pattern INSERT_START = Pattern.compile("^\\s*INSERT\\s+INTO\\s+`([^`]+)`",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMNS_LIST = Pattern.compile(
            "^\\s*INSERT\\s+INTO\\s+`[^`]+`\\s*\\((.+?)\\)\\s*VALUES\\s*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Parse all INSERT statements from a reader.
     * Returns an iterable that yields complete INSERT statements.
     */
    public Iterable<String> parseInserts(BufferedReader reader) {
        return () -> new Iterator<>() {
            private String nextInsert;
            private StringBuilder currentInsert;
            private boolean inInsert = false;
            private int parenDepth = 0;
            private boolean inSingleQuote = false;
            private boolean inDoubleQuote = false;
            private boolean escapeNext = false;

            {
                advance();
            }

            @Override
            public boolean hasNext() {
                return nextInsert != null;
            }

            @Override
            public String next() {
                if (nextInsert == null) {
                    throw new NoSuchElementException();
                }
                String result = nextInsert;
                advance();
                return result;
            }

            private void advance() {
                nextInsert = null;
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!inInsert && line.toUpperCase().contains("INSERT INTO")) {
                            currentInsert = new StringBuilder(line).append('\n');
                            inInsert = true;
                            inSingleQuote = false;
                            inDoubleQuote = false;
                            escapeNext = false;
                            parenDepth = 0;

                            processLine(line);

                            if (line.trim().endsWith(";") && parenDepth == 0) {
                                nextInsert = currentInsert.toString();
                                currentInsert = null;
                                inInsert = false;
                                return;
                            }
                            continue;
                        }

                        if (inInsert) {
                            currentInsert.append(line).append('\n');
                            processLine(line);

                            if (line.trim().endsWith(";") && parenDepth == 0) {
                                nextInsert = currentInsert.toString();
                                currentInsert = null;
                                inInsert = false;
                                return;
                            }
                        }
                    }

                    // Handle incomplete statement at EOF
                    if (inInsert && currentInsert != null) {
                        nextInsert = currentInsert.toString();
                        currentInsert = null;
                        inInsert = false;
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            private void processLine(String line) {
                for (char c : line.toCharArray()) {
                    if (escapeNext) {
                        escapeNext = false;
                        continue;
                    }
                    if (c == '\\') {
                        escapeNext = true;
                        continue;
                    }
                    if (c == '\'' && !inDoubleQuote) {
                        inSingleQuote = !inSingleQuote;
                    } else if (c == '"' && !inSingleQuote) {
                        inDoubleQuote = !inDoubleQuote;
                    } else if (!inSingleQuote && !inDoubleQuote) {
                        if (c == '(')
                            parenDepth++;
                        else if (c == ')')
                            parenDepth--;
                    }
                }
            }
        };
    }

    /**
     * Expand a multi-row INSERT statement into individual rows.
     */
    public List<InsertRow> expandInsert(String insertStmt, Map<String, List<String>> columnsMap) {
        String normalized = insertStmt.replaceAll("\\s+", " ").trim();

        Matcher tableMatcher = INSERT_START.matcher(normalized);
        if (!tableMatcher.find()) {
            return List.of();
        }
        String table = tableMatcher.group(1);

        List<String> columns = null;
        int valuesStartPos = 0;
        boolean hasExplicitCols = false;

        Matcher colMatcher = COLUMNS_LIST.matcher(normalized);
        if (colMatcher.find()) {
            String colsStr = colMatcher.group(1);
            columns = Arrays.stream(colsStr.split(","))
                    .map(col -> col.trim().replaceAll("[`'\"]", ""))
                    .toList();
            valuesStartPos = colMatcher.end();
            hasExplicitCols = true;
        } else {
            columns = columnsMap.get(table);
            int valuesIdx = normalized.toUpperCase().indexOf("VALUES");
            if (valuesIdx == -1) {
                return List.of();
            }
            valuesStartPos = valuesIdx + 6; // len("VALUES")
        }

        if (columns == null || columns.isEmpty()) {
            return List.of();
        }

        String valuesPart = normalized.substring(valuesStartPos).trim();
        if (valuesPart.endsWith(";")) {
            valuesPart = valuesPart.substring(0, valuesPart.length() - 1);
        }

        if (valuesPart.isEmpty()) {
            return List.of();
        }

        List<String> groups = splitValueGroups(valuesPart);
        List<InsertRow> results = new ArrayList<>();

        for (String group : groups) {
            String inner = group.trim();
            if (inner.startsWith("(") && inner.endsWith(")")) {
                inner = inner.substring(1, inner.length() - 1);
            }

            List<String> values = parseValues(inner);
            if (values.size() != columns.size()) {
                continue; // Skip malformed row
            }

            Map<String, String> data = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                data.put(columns.get(i), values.get(i));
            }

            String stmt = hasExplicitCols
                    ? String.format("INSERT INTO `%s` (%s) VALUES %s;",
                            table,
                            columns.stream().map(c -> "`" + c + "`").reduce((a, b) -> a + ", " + b).orElse(""),
                            group)
                    : String.format("INSERT INTO `%s` VALUES %s;", table, group);

            results.add(new InsertRow(table, columns, data, stmt));
        }

        return results;
    }

    private List<String> splitValueGroups(String valuesPart) {
        List<String> groups = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean escapeNext = false;
        int parenDepth = 0;

        for (char c : valuesPart.toCharArray()) {
            if (escapeNext) {
                buf.append(c);
                escapeNext = false;
                continue;
            }
            if (c == '\\') {
                buf.append(c);
                escapeNext = true;
                continue;
            }
            if (c == '\'' && !inDoubleQuote) {
                buf.append(c);
                inSingleQuote = !inSingleQuote;
                continue;
            }
            if (c == '"' && !inSingleQuote) {
                buf.append(c);
                inDoubleQuote = !inDoubleQuote;
                continue;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') {
                    parenDepth++;
                    buf.append(c);
                    continue;
                }
                if (c == ')') {
                    parenDepth--;
                    buf.append(c);
                    if (parenDepth == 0) {
                        groups.add(buf.toString().trim());
                        buf = new StringBuilder();
                    }
                    continue;
                }
                if (c == ',' && parenDepth == 0) {
                    continue; // Skip separator between groups
                }
            }
            buf.append(c);
        }

        if (!buf.isEmpty()) {
            String leftover = buf.toString().trim();
            if (!leftover.isEmpty()) {
                groups.add(leftover);
            }
        }

        return groups;
    }

    private List<String> parseValues(String valuesStr) {
        try {
            // Use Apache Commons CSV with SQL-style quoting
            CSVFormat format = CSVFormat.DEFAULT.builder()
                    .setQuote('\'')
                    .setEscape('\\')
                    .setIgnoreSurroundingSpaces(true)
                    .build();

            CSVParser parser = CSVParser.parse(valuesStr, format);
            CSVRecord record = parser.iterator().next();

            List<String> values = new ArrayList<>();
            for (String value : record) {
                values.add(value);
            }
            return values;
        } catch (Exception e) {
            // Fallback to simple split if CSV parsing fails
            return Arrays.asList(valuesStr.split(","));
        }
    }
}
