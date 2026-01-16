package com.sqldumpdiff;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lombok.extern.java.Log;

/**
 * Compares old and new table data to generate delta SQL.
 * Uses a binary on-disk store for memory-efficient storage of old records.
 */
@Log
public class TableComparer {
    private final SqliteProfile profile;

    public TableComparer(SqliteProfile profile) {
        this.profile = profile;
    }

    public TableCompareResult compare(TableComparison comparison) throws IOException {
        if (comparison.pkColumns() == null) {
            return new TableCompareResult(
                    new ComparisonResult(comparison.tableName(), "", "", 0, 0, 0),
                    new TableTiming(comparison.tableName(), 0, 0, 0));
        }

        // Use a binary store instead of SQLite for memory efficiency
        BinaryTableStore oldStore = null;

        long loadMs = 0;
        long compareMs = 0;
        long deleteMs = 0;

        try {
            // Load old records into SQLite
            if (comparison.oldFile() != null && Files.exists(comparison.oldFile())) {
                long start = System.nanoTime();
                oldStore = loadTableFileToStore(comparison.oldFile(), comparison.tableName(), comparison.pkColumns());
                loadMs = (System.nanoTime() - start) / 1_000_000L;

                long oldCount = oldStore != null ? oldStore.getRowCount() : 0;
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
                long start = System.nanoTime();
                try (TableBinIO.TableBinReader reader = TableBinIO.openReader(comparison.newFile())) {
                    long fileSize = Files.size(comparison.newFile());
                    int baseBatch = profile != null ? profile.batch() : 1000;
                    int batchSize = adjustBatchByFileSize(baseBatch, fileSize);
                    List<String> pkBatch = new ArrayList<>(batchSize);
                    List<InsertRow> rowBatch = new ArrayList<>(batchSize);
                    List<String> columns = reader.columns();
                    List<String> values;
                    while ((values = reader.readRow()) != null) {
                        InsertRow newRow = buildRow(comparison.tableName(), columns, values);
                        List<String> pkValues = getPkValues(newRow, comparison.pkColumns());
                        String pkHash = BinaryTableStore.hashPrimaryKeyValues(pkValues);
                        pkBatch.add(pkHash);
                        rowBatch.add(newRow);

                        if (pkBatch.size() >= batchSize) {
                        int[] counts = updateCounts(comparison, oldStore, pkBatch, rowBatch, changes,
                                matchedOld);
                            insertCount += counts[0];
                            updateCount += counts[1];
                            pkBatch.clear();
                            rowBatch.clear();
                        }
                    }
                    if (!pkBatch.isEmpty()) {
                        int[] counts = updateCounts(comparison, oldStore, pkBatch, rowBatch, changes, matchedOld);
                        insertCount += counts[0];
                        updateCount += counts[1];
                    }
                }
                compareMs = (System.nanoTime() - start) / 1_000_000L;
            }

            // Handle deletions: emit any old row hashes that were never matched
            long deleteStart = System.nanoTime();
            if (oldStore != null) {
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
            }
            deleteMs = (System.nanoTime() - deleteStart) / 1_000_000L;

            ComparisonResult result = new ComparisonResult(
                    comparison.tableName(),
                    changes.toString(),
                    deletions.toString(),
                    insertCount,
                    updateCount,
                    deleteCount);
            TableTiming timing = new TableTiming(comparison.tableName(), loadMs, compareMs, deleteMs);
            return new TableCompareResult(result, timing);
        } finally {
            if (oldStore != null) {
                oldStore.close();
            }
        }
    }

    private BinaryTableStore loadTableFileToStore(Path file, String table, List<String> pkColumns)
            throws IOException {
        BinaryTableStore store = null;
        try (TableBinIO.TableBinReader reader = TableBinIO.openReader(file)) {
            int batch = 0;
            List<String> columns = reader.columns();
            store = new BinaryTableStore(table, columns, pkColumns);
            List<String> values;
            while ((values = reader.readRow()) != null) {
                InsertRow row = buildRow(table, columns, values);
                store.insertRow(row);

                int batchSize = profile != null ? profile.batch() : 1000;
                if (++batch % batchSize == 0) {
                    store.executeBatch();
                }
            }
            store.executeBatch();
        }
        return store;
    }

    private List<String> getPkValues(InsertRow row, List<String> pkColumns) {
        List<String> result = new ArrayList<>(pkColumns.size());
        for (String col : pkColumns) {
            result.add(row.data().get(col));
        }
        return result;
    }

    private int[] updateCounts(TableComparison comparison, BinaryTableStore oldStore, List<String> pkBatch,
            List<InsertRow> rowBatch, StringBuilder changes, Set<String> matchedOld) throws IOException {
        int insertCount = 0;
        int updateCount = 0;
        Map<String, Map<String, String>> oldDataBatch = oldStore == null
                ? java.util.Collections.emptyMap()
                : oldStore.getRowDataBatch(pkBatch);
        for (int i = 0; i < rowBatch.size(); i++) {
            InsertRow newRow = rowBatch.get(i);
            String pkHash = pkBatch.get(i);
            Map<String, String> oldData = oldDataBatch.get(pkHash);
            if (oldData == null) {
                changes.append("-- NEW RECORD IN ").append(comparison.tableName()).append("\n");
                changes.append(buildInsertStatement(comparison.tableName(), newRow.columns(), newRow.data()))
                        .append("\n\n");
                insertCount++;
                continue;
            }
            matchedOld.add(pkHash);

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
        return new int[] { insertCount, updateCount };
    }

    private int adjustBatchByFileSize(int base, long sizeBytes) {
        if (base <= 0) {
            base = 1000;
        }
        double mb = sizeBytes / (1024.0 * 1024.0);
        int scaled;
        if (mb < 64) {
            scaled = base;
        } else if (mb < 256) {
            scaled = Math.max(64, base / 2);
        } else if (mb < 1024) {
            scaled = Math.max(64, base / 4);
        } else if (mb < 4096) {
            scaled = Math.max(64, base / 8);
        } else {
            scaled = Math.max(64, base / 16);
        }
        return scaled;
    }

    private String normalizeNull(String val) {
        if (val == null) {
            return null;
        }

        String trimmed = val.trim();
        if ("NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }

        // If the value is enclosed in single quotes, strip them and unescape doubled
        // quotes
        if (trimmed.length() >= 2 && trimmed.startsWith("'") && trimmed.endsWith("'")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
            trimmed = trimmed.replace("''", "'");
        }

        return trimmed;
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

    private InsertRow buildRow(String table, List<String> columns, List<String> values) {
        Map<String, String> data = new java.util.HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            data.put(columns.get(i), values.get(i));
        }
        return new InsertRow(table, columns, data, "");
    }

    private String buildInsertStatement(String table, List<String> columns, Map<String, String> data) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO `").append(table).append("` (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append('`').append(columns.get(i)).append('`');
        }
        sb.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            String val = data.get(columns.get(i));
            if (val == null || "NULL".equalsIgnoreCase(val)) {
                sb.append("NULL");
            } else {
                sb.append(val);
            }
        }
        sb.append(");");
        return sb.toString();
    }
}
