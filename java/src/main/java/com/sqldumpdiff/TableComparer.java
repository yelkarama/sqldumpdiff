package com.sqldumpdiff;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Compares old and new table data to generate delta SQL.
 */
public class TableComparer {

    public ComparisonResult compare(TableComparison comparison) throws IOException {
        if (comparison.pkColumns() == null) {
            return new ComparisonResult(comparison.tableName(), "", "", 0, 0, 0);
        }

        // Store only the data map, not the full InsertRow to save memory
        Map<List<String>, Map<String, String>> oldRecords = loadTableFile(
                comparison.oldFile(), comparison.tableName(), comparison.pkColumns());

        StringBuilder changes = new StringBuilder();
        StringBuilder deletions = new StringBuilder();
        Set<List<String>> matchedOld = new HashSet<>();
        int insertCount = 0;
        int updateCount = 0;
        int deleteCount = 0;

        // Process new rows
        if (comparison.newFile() != null && Files.exists(comparison.newFile())) {
            try (BufferedReader reader = Files.newBufferedReader(comparison.newFile())) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty())
                        continue;

                    InsertRow newRow = InsertRow.fromJson(line, comparison.tableName());
                    List<String> pkValues = getPkValues(newRow, comparison.pkColumns());

                    Map<String, String> oldData = oldRecords.get(pkValues);

                    if (oldData == null) {
                        // New record
                        changes.append("-- NEW RECORD IN ").append(comparison.tableName()).append("\n");
                        changes.append(newRow.statement()).append("\n\n");
                        insertCount++;
                        continue;
                    }

                    matchedOld.add(pkValues);

                    // Check for updates
                    List<String> updates = new ArrayList<>();
                    List<String> comments = new ArrayList<>();

                    for (String col : newRow.columns()) {
                        String oldVal = normalizeNull(oldData.get(col));
                        String newVal = normalizeNull(newRow.data().get(col));

                        if (!Objects.equals(oldVal, newVal)) {
                            comments.add("-- " + col + " old value: " + oldData.get(col));

                            if (newVal == null) {
                                updates.add("`" + col + "`=NULL");
                            } else {
                                String escaped = newVal.replace("'", "''");
                                updates.add("`" + col + "`='" + escaped + "'");
                            }
                        }
                    }

                    if (!updates.isEmpty()) {
                        for (String comment : comments) {
                            changes.append(comment).append("\n");
                        }

                        String whereClause = buildWhereClause(comparison.pkColumns(), newRow.data());
                        changes.append("UPDATE `").append(comparison.tableName()).append("` SET ");
                        changes.append(String.join(", ", updates));
                        changes.append(" WHERE ").append(whereClause).append(";\n\n");
                        updateCount++;
                    }
                }
            }
        }

        // Handle deletions
        for (Map.Entry<List<String>, Map<String, String>> entry : oldRecords.entrySet()) {
            if (matchedOld.contains(entry.getKey())) {
                continue;
            }

            Map<String, String> oldData = entry.getValue();
            String whereClause = buildWhereClause(comparison.pkColumns(), oldData);

            deletions.append("-- DELETED FROM ").append(comparison.tableName());
            deletions.append(": ").append(entry.getKey()).append("\n");
            deletions.append("DELETE FROM `").append(comparison.tableName()).append("`");
            deletions.append(" WHERE ").append(whereClause).append(";\n\n");
            deleteCount++;
        }

        return new ComparisonResult(
                comparison.tableName(),
                changes.toString(),
                deletions.toString(),
                insertCount,
                updateCount,
                deleteCount);
    }

    private Map<List<String>, Map<String, String>> loadTableFile(Path file, String table, List<String> pkColumns) throws IOException {
        Map<List<String>, Map<String, String>> records = new HashMap<>();

        if (file == null || !Files.exists(file)) {
            return records;
        }

        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty())
                    continue;

                InsertRow row = InsertRow.fromJson(line, table);
                List<String> pkValues = getPkValues(row, pkColumns);
                // Store only the data map to reduce memory footprint
                records.put(pkValues, row.data());
            }
        }

        return records;
    }

    private List<String> getPkValues(InsertRow row, List<String> pkColumns) {
        List<String> result = new ArrayList<>(pkColumns.size());
        for (String col : pkColumns) {
            result.add(row.data().get(col));
        }
        return result;
    }

    private String normalizeNull(String val) {
        if (val == null || "NULL".equalsIgnoreCase(val)) {
            return null;
        }
        return val;
    }

    private String buildWhereClause(List<String> pkColumns, Map<String, String> data) {
        List<String> parts = new ArrayList<>();

        for (String col : pkColumns) {
            String val = data.get(col);
            if (val == null || "NULL".equalsIgnoreCase(val)) {
                parts.add("`" + col + "` IS NULL");
            } else {
                String escaped = val.replace("'", "''");
                parts.add("`" + col + "`='" + escaped + "'");
            }
        }

        return String.join(" AND ", parts);
    }
}
