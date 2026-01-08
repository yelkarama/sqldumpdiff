package com.sqldumpdiff;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lombok.extern.java.Log;

/**
 * Compares old and new table data to generate delta SQL.
 * Uses SQLite for memory-efficient storage of old records instead of HashMap.
 */
@Log
public class TableComparer {

    public ComparisonResult compare(TableComparison comparison) throws IOException, SQLException {
        if (comparison.pkColumns() == null) {
            return new ComparisonResult(comparison.tableName(), "", "", 0, 0, 0);
        }

        // Use SQLite instead of HashMap for memory efficiency
        SQLiteTableStore oldStore = new SQLiteTableStore(comparison.tableName(), comparison.pkColumns());

        try {
            // Load old records into SQLite
            if (comparison.oldFile() != null && Files.exists(comparison.oldFile())) {
                loadTableFileToSQLite(comparison.oldFile(), comparison.tableName(), oldStore);
                oldStore.analyzeForQuery(); // Optimize query plan after loading

                long oldCount = oldStore.getRowCount();
                if (oldCount == 0) {
                    log.fine(() -> "Table " + comparison.tableName() + ": No old rows loaded. PK columns: " +
                            String.join(", ", comparison.pkColumns()));
                }
            }

            StringBuilder changes = new StringBuilder();
            StringBuilder deletions = new StringBuilder();
            Set<String> matchedOld = new HashSet<>();
            int insertCount = 0;
            int updateCount = 0;
            int deleteCount = 0;

            // Process new rows
            if (comparison.newFile() != null && Files.exists(comparison.newFile())) {
                try (BufferedReader reader = Files.newBufferedReader(comparison.newFile())) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.trim().isEmpty()) {
                            continue;
                        }

                        InsertRow newRow = InsertRow.fromJson(line, comparison.tableName());
                        List<String> pkValues = getPkValues(newRow, comparison.pkColumns());
                        String pkHash = SQLiteTableStore.hashPrimaryKeyValues(pkValues);

                        // Query SQLite for old record matching this PK
                        Map<String, String> oldData = oldStore.getRowData(pkHash);

                        if (oldData == null) {
                            // New record
                            changes.append("-- NEW RECORD IN ").append(comparison.tableName()).append("\n");
                            changes.append(newRow.statement()).append("\n\n");
                            insertCount++;
                            continue;
                        }

                        matchedOld.add(pkHash);

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

            // Handle deletions: emit any old row hashes that were never matched
            Set<String> allOldHashes = oldStore.getAllPkHashes();
            for (String pkHash : allOldHashes) {
                if (matchedOld.contains(pkHash)) {
                    continue;
                }

                Map<String, String> oldData = oldStore.getRowData(pkHash);
                String whereClause = buildWhereClause(comparison.pkColumns(), oldData);

                deletions.append("-- DELETED FROM ").append(comparison.tableName());
                deletions.append(": ").append(pkHash).append("\n");
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
        } finally {
            oldStore.close();
        }
    }

    private void loadTableFileToSQLite(Path file, String table, SQLiteTableStore store)

            throws IOException, SQLException {
        // Read rows and insert into SQLite
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String line;
            int batch = 0;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                InsertRow row = InsertRow.fromJson(line, table);
                store.insertRow(row);

                // Execute batch every 1k rows for better memory management
                if (++batch % 1000 == 0) {
                    store.executeBatch();
                }
            }
            // Final batch
            store.executeBatch();
        }
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
