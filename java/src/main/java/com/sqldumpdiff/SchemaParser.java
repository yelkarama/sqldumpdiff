package com.sqldumpdiff;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/**
 * Parses SQL schemas to extract PRIMARY KEY definitions and column orders.
 */
public class SchemaParser {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("CREATE TABLE `(.+?)`", Pattern.CASE_INSENSITIVE);
    private static final Pattern PK_PATTERN = Pattern.compile("PRIMARY\\s+KEY\\s*\\((.+?)\\)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    public Map<String, List<String>> parseSchemas(Path file) throws IOException {
        Map<String, List<String>> schemaMap = new HashMap<>();

        StringBuilder currentTable = new StringBuilder();
        String tableName = null;
        boolean inCreateTable = false;

        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher tableMatcher = TABLE_NAME_PATTERN.matcher(line);
                if (tableMatcher.find()) {
                    // Process previous table if any
                    if (inCreateTable && tableName != null) {
                        extractPrimaryKey(tableName, currentTable.toString(), schemaMap);
                    }

                    tableName = tableMatcher.group(1);
                    currentTable = new StringBuilder(line).append('\n');
                    inCreateTable = true;
                    continue;
                }

                if (inCreateTable) {
                    currentTable.append(line).append('\n');
                    if (line.trim().endsWith(";")) {
                        extractPrimaryKey(tableName, currentTable.toString(), schemaMap);
                        tableName = null;
                        currentTable = new StringBuilder();
                        inCreateTable = false;
                    }
                }
            }

            // Handle last table if file doesn't end with semicolon
            if (inCreateTable && tableName != null) {
                extractPrimaryKey(tableName, currentTable.toString(), schemaMap);
            }
        }

        return schemaMap;
    }

    public Map<String, List<String>> parseColumns(Path file) throws IOException {
        Map<String, List<String>> columnsMap = new HashMap<>();

        StringBuilder currentTable = new StringBuilder();
        String tableName = null;
        boolean inCreateTable = false;

        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher tableMatcher = TABLE_NAME_PATTERN.matcher(line);
                if (tableMatcher.find()) {
                    if (inCreateTable && tableName != null) {
                        columnsMap.put(tableName, extractColumns(currentTable.toString()));
                    }

                    tableName = tableMatcher.group(1);
                    currentTable = new StringBuilder(line).append('\n');
                    inCreateTable = true;
                    continue;
                }

                if (inCreateTable) {
                    currentTable.append(line).append('\n');
                    if (line.trim().endsWith(";")) {
                        columnsMap.put(tableName, extractColumns(currentTable.toString()));
                        tableName = null;
                        currentTable = new StringBuilder();
                        inCreateTable = false;
                    }
                }
            }

            if (inCreateTable && tableName != null) {
                columnsMap.put(tableName, extractColumns(currentTable.toString()));
            }
        }

        return columnsMap;
    }

    private void extractPrimaryKey(String tableName, String content, Map<String, List<String>> schemaMap) {
        Matcher pkMatcher = PK_PATTERN.matcher(content);
        if (pkMatcher.find()) {
            String pkDef = pkMatcher.group(1);
            pkDef = pkDef.replaceAll("\\s+", " ").trim();

            List<String> columns = Arrays.stream(pkDef.split(","))
                    .map(col -> col.trim().replaceAll("[`'\"]", ""))
                    .toList();

            schemaMap.put(tableName, columns);
        }
    }

    private List<String> extractColumns(String content) {
        // Extract content between first '(' and matching ')'
        int start = content.indexOf('(');
        if (start == -1)
            return List.of();

        int depth = 0;
        int end = -1;
        boolean inQuote = false;
        char quoteChar = 0;

        for (int i = start; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '\\' && i + 1 < content.length()) {
                i++; // Skip escaped character
                continue;
            }

            if (!inQuote && (c == '\'' || c == '"')) {
                inQuote = true;
                quoteChar = c;
            } else if (inQuote && c == quoteChar) {
                inQuote = false;
            } else if (!inQuote) {
                if (c == '(')
                    depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0) {
                        end = i;
                        break;
                    }
                }
            }
        }

        if (end == -1)
            return List.of();

        String inner = content.substring(start + 1, end);
        List<String> columns = new ArrayList<>();

        // Split by top-level commas
        int commaDepth = 0;
        inQuote = false;
        quoteChar = 0;
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);

            if (c == '\\' && i + 1 < inner.length()) {
                current.append(c).append(inner.charAt(++i));
                continue;
            }

            if (!inQuote && (c == '\'' || c == '"')) {
                inQuote = true;
                quoteChar = c;
                current.append(c);
            } else if (inQuote && c == quoteChar) {
                inQuote = false;
                current.append(c);
            } else if (!inQuote) {
                if (c == '(') {
                    commaDepth++;
                    current.append(c);
                } else if (c == ')') {
                    commaDepth--;
                    current.append(c);
                } else if (c == ',' && commaDepth == 0) {
                    String item = current.toString().trim();
                    if (!item.isEmpty()) {
                        extractColumnName(item).ifPresent(columns::add);
                    }
                    current = new StringBuilder();
                } else {
                    current.append(c);
                }
            } else {
                current.append(c);
            }
        }

        // Add last item
        if (!current.isEmpty()) {
            String item = current.toString().trim();
            extractColumnName(item).ifPresent(columns::add);
        }

        return columns;
    }

    private Optional<String> extractColumnName(String item) {
        // Skip constraints
        if (item.matches("(?i)^(PRIMARY\\s+KEY|UNIQUE\\s+KEY|KEY|CONSTRAINT|FOREIGN\\s+KEY)\\b.*")) {
            return Optional.empty();
        }

        // Extract column name between backticks
        Pattern colPattern = Pattern.compile("^`([^`]+)`\\s+");
        Matcher matcher = colPattern.matcher(item);
        if (matcher.find()) {
            return Optional.of(matcher.group(1));
        }

        return Optional.empty();
    }
}
