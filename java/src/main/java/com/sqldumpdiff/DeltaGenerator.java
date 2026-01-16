package com.sqldumpdiff;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.extern.java.Log;
import me.tongfei.progressbar.ProgressBar;

/**
 * Main delta generation logic using virtual threads for maximum concurrency.
 */
@Log
public class DeltaGenerator {

    public void generateDelta(String oldFilePath, String newFilePath, String outputPath, boolean debug, boolean timing, String timingJsonPath, Instant startTime, Instant wallStart, SqliteProfile profile)
            throws IOException, InterruptedException, ExecutionException {

        Path oldFile = Path.of(oldFilePath);
        Path newFile = Path.of(newFilePath);

        if (!Files.exists(oldFile)) {
            throw new FileNotFoundException("Old file not found: " + oldFilePath);
        }
        if (!Files.exists(newFile)) {
            throw new FileNotFoundException("New file not found: " + newFilePath);
        }

        if (!debug) {
            System.err.println("Step 1: Parsing schemas from DDL...");
        }
        log.info("Step 1: Parsing schemas from DDL...");
        Instant schemaStart = Instant.now();
        long schemaMs = 0;

        SchemaParser schemaParser = new SchemaParser();
        Map<String, List<String>> pkMap;
        Map<String, List<String>> oldColumnsMap;
        Map<String, List<String>> newColumnsMap;

        ProgressBarFactory pbFactory = new ProgressBarFactory(debug);

        try (ProgressBar pb = pbFactory.create("Parsing schemas", 3)) {
            pkMap = schemaParser.parseSchemas(newFile);
            pb.step();
            oldColumnsMap = schemaParser.parseColumns(oldFile);
            pb.step();
            newColumnsMap = schemaParser.parseColumns(newFile);
            pb.step();
        }

        schemaMs = Duration.between(schemaStart, Instant.now()).toMillis();
        if (pkMap.isEmpty()) {
            System.err.println("Warning: No tables with PRIMARY KEY found in new file schema.");
        }

        if (!debug) {
            System.err.println("Step 2: Splitting dumps by table using virtual threads...");
        }
        log.info("Step 2: Splitting dumps by table using virtual threads...");
        Instant splitStart = Instant.now();
        long splitMs = 0;

        Path tempDir = Files.createTempDirectory("sqldumpdiff_");
        try {
            // Split both files in parallel using virtual threads
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                long oldFileSize = Files.size(oldFile);
                long newFileSize = Files.size(newFile);

                Map<String, Path> oldTableFiles;
                Map<String, Path> newTableFiles;

                try (ProgressBar oldPb = pbFactory.create("Splitting old dump", oldFileSize, " bytes");
                        ProgressBar newPb = pbFactory.create("Splitting new dump", newFileSize, " bytes")) {

                    Future<Map<String, Path>> oldFuture = executor
                            .submit(() -> splitDumpByTable(oldFile, oldColumnsMap, pkMap, tempDir, "old", oldPb));
                    Future<Map<String, Path>> newFuture = executor
                            .submit(() -> splitDumpByTable(newFile, newColumnsMap, pkMap, tempDir, "new", newPb));

                    oldTableFiles = oldFuture.get();
                    newTableFiles = newFuture.get();
                }
                splitMs = Duration.between(splitStart, Instant.now()).toMillis();
                if (debug || timing) {
                    log.info("Timing: split dumps took " + splitMs + "ms");
                }

                // Flush stderr to ensure progress bars are fully rendered before continuing
                System.err.flush();

                Set<String> allTables = new HashSet<>();
                allTables.addAll(oldTableFiles.keySet());
                allTables.addAll(newTableFiles.keySet());

                if (allTables.isEmpty()) {
                    log.warning("No tables with primary keys found to process.");
                    return;
                }

                log.log(java.util.logging.Level.INFO, "Step 3: Comparing {0} tables using virtual threads...",
                        allTables.size());

                // Process all tables in parallel with virtual threads
                List<TableComparison> comparisons = allTables.stream()
                        .map(table -> new TableComparison(
                                table,
                                pkMap.get(table),
                                oldTableFiles.get(table),
                                newTableFiles.get(table)))
                        .toList();

                List<ComparisonResult> results = new ArrayList<>();
                List<TableTiming> timings = new ArrayList<>();
                Instant compareStart = Instant.now();
                long compareMs = 0;
                try (ProgressBar pb = pbFactory.create("Comparing tables", comparisons.size());
                        ExecutorService compareExecutor = Executors.newVirtualThreadPerTaskExecutor()) {

                    int maxWorkers = profile != null && profile.workers() > 0
                            ? profile.workers()
                            : Runtime.getRuntime().availableProcessors();
                    java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore(maxWorkers);

                    List<Future<TableCompareResult>> futures = comparisons.stream()
                            .map(comp -> compareExecutor.submit(() -> {
                                sem.acquire();
                                try {
                                    TableCompareResult result = compareTable(comp, profile);
                                    pb.step();
                                    return result;
                                } finally {
                                    sem.release();
                                }
                            }))
                            .toList();

                    for (Future<TableCompareResult> future : futures) {
                        TableCompareResult res = future.get();
                        results.add(res.result());
                        timings.add(res.timing());
                    }
                }
                compareMs = Duration.between(compareStart, Instant.now()).toMillis();
                if (debug || timing) {
                    log.info("Timing: compare tables took " + compareMs + "ms");
                }

                log.info("Step 4: Writing delta script...");

                int totalChanges = results.stream()
                        .mapToInt(r -> r.insertCount() + r.updateCount() + r.deleteCount())
                        .sum();

                Instant writeStart = Instant.now();
                long writeMs = 0;
                long writeFormatMs = 0;
                long writeIoMs = 0;
                try (ProgressBar pb = pbFactory.create("Writing delta SQL", Math.max(totalChanges, 1))) {
                    WriteTiming wt = writeDeltaScript(results, outputPath, pb);
                    writeFormatMs = wt.formatMs();
                    writeIoMs = wt.ioMs();
                }
                writeMs = Duration.between(writeStart, Instant.now()).toMillis();
                if (debug || timing) {
                    log.info("Timing: write delta SQL took " + writeMs + "ms");
                }

                // Print summary
                int totalInserts = results.stream().mapToInt(ComparisonResult::insertCount).sum();
                int totalUpdates = results.stream().mapToInt(ComparisonResult::updateCount).sum();
                int totalDeletes = results.stream().mapToInt(ComparisonResult::deleteCount).sum();

                String summary = String.format(
                        "\n%s\nSUMMARY\n%s\nInserts:  %,d\nUpdates:  %,d\nDeletes:  %,d\nTotal:    %,d\n%s",
                        "=".repeat(60), "=".repeat(60),
                        totalInserts, totalUpdates, totalDeletes,
                        totalInserts + totalUpdates + totalDeletes,
                        "=".repeat(60));
                System.err.println(summary);

                if (debug || timing) {
                    timings.sort(Comparator.comparingLong(TableTiming::totalMs).reversed());
                    int limit = Math.min(10, timings.size());
                    if (limit > 0) {
                        log.info("Timing: top " + limit + " slowest tables (ms):");
                        for (int i = 0; i < limit; i++) {
                            TableTiming t = timings.get(i);
                            log.info(String.format("  %s total=%d load=%d compare=%d delete=%d",
                                    t.tableName(), t.totalMs(), t.loadMs(), t.compareMs(), t.deleteMs()));
                        }
                    }
                }

                if (timingJsonPath != null && !timingJsonPath.isBlank()) {
                    long wallMs = Duration.between(wallStart, Instant.now()).toMillis();
                    writeTimingJsonReport(timingJsonPath, timings, schemaMs, splitMs, compareMs, writeMs, writeFormatMs, writeIoMs, wallMs);
                }
                log.log(java.util.logging.Level.INFO, "Completed with {0} inserts, {1} updates, {2} deletes",
                        new Object[] { totalInserts, totalUpdates, totalDeletes });
            }
        } finally {
            // Cleanup temp directory
            deleteDirectory(tempDir);
        }
    }

    private Map<String, Path> splitDumpByTable(
            Path dumpFile,
            Map<String, List<String>> columnsMap,
            Map<String, List<String>> pkMap,
            Path tempDir,
            String label,
            ProgressBar progressBar) throws IOException {

        InsertParser parser = new InsertParser();
        Map<String, TableBinIO.TableBinWriter> writers = new HashMap<>();
        Map<String, Path> tablePaths = new HashMap<>();
        Map<String, Long> rowCounts = new HashMap<>();
        long bytesRead = 0;

        try (BufferedReader reader = Files.newBufferedReader(dumpFile)) {
            for (String insertStmt : parser.parseInserts(reader)) {
                bytesRead += insertStmt.length();
                if (progressBar != null) {
                    progressBar.stepTo(Math.min(bytesRead, progressBar.getMax()));
                }

                List<InsertRow> rows = parser.expandInsert(insertStmt, columnsMap);

                for (InsertRow row : rows) {
                    if (!pkMap.containsKey(row.table())) {
                        continue;
                    }

                    List<String> pkCols = pkMap.get(row.table());
                    boolean hasAllPkCols = pkCols.stream()
                            .allMatch(col -> row.data().containsKey(col));

                    if (!hasAllPkCols) {
                        continue;
                    }

                    if (!writers.containsKey(row.table())) {
                        List<String> cols = columnsMap.getOrDefault(row.table(), row.columns());
                        if (cols == null || cols.isEmpty()) {
                            continue;
                        }
                        Path tablePath = tempDir.resolve(label + "_" + sanitizeFilename(row.table()) + ".bin");
                        tablePaths.put(row.table(), tablePath);
                        writers.put(row.table(), TableBinIO.openWriter(tablePath, cols));
                        rowCounts.put(row.table(), 0L);
                        log.log(java.util.logging.Level.FINE, "Processing {0} table: {1}",
                                new Object[] { label, row.table() });
                    }

                    writers.get(row.table()).writeRow(row.data());
                    rowCounts.put(row.table(), rowCounts.get(row.table()) + 1);
                }
            }

            // Log row counts for each table
            for (Map.Entry<String, Long> entry : rowCounts.entrySet()) {
                log.log(java.util.logging.Level.FINE, "Table {0} ({1}): {2} rows",
                        new Object[] { entry.getKey(), label, entry.getValue() });
            }

            // Ensure progress bar reaches 100%
            if (progressBar != null) {
                progressBar.stepTo(progressBar.getMax());
            }
        } finally {
            for (TableBinIO.TableBinWriter writer : writers.values()) {
                writer.close();
            }
        }

        return tablePaths;
    }

    private TableCompareResult compareTable(TableComparison comparison, SqliteProfile profile) {
        try {
            TableComparer comparer = new TableComparer(profile);
            TableCompareResult result = comparer.compare(comparison);

            // Log detailed comparison results
            ComparisonResult res = result.result();
            if (res.insertCount() > 0 || res.updateCount() > 0 || res.deleteCount() > 0) {
                log.log(java.util.logging.Level.FINE, "Table {0}: {1} inserts, {2} updates, {3} deletes",
                        new Object[] { res.tableName(), res.insertCount(), res.updateCount(),
                                res.deleteCount() });
            }

            return result;
        } catch (IOException e) {
            System.err.println("Error comparing table " + comparison.tableName() + ": " + e.getMessage());
            return new TableCompareResult(
                    new ComparisonResult(comparison.tableName(), "", "", 0, 0, 0),
                    new TableTiming(comparison.tableName(), 0, 0, 0));
        }
    }

    private WriteTiming writeDeltaScript(List<ComparisonResult> results, String outputPath, ProgressBar progressBar)
            throws IOException {
        Writer out = outputPath != null
                ? Files.newBufferedWriter(Path.of(outputPath))
                : new OutputStreamWriter(System.out);

        long formatMs = 0;
        long ioMs = 0;

        try (Writer _ = outputPath != null ? out : null) {
            long fmtStart = System.nanoTime();
            StringBuilder header = new StringBuilder();
            header.append("-- Full Delta Update Script\n");
            header.append("SET FOREIGN_KEY_CHECKS = 0;\n\n");
            formatMs += (System.nanoTime() - fmtStart) / 1_000_000L;

            long ioStart = System.nanoTime();
            out.write(header.toString());
            ioMs += (System.nanoTime() - ioStart) / 1_000_000L;

            // Sort by table name for deterministic output
            results.sort(Comparator.comparing(ComparisonResult::tableName));

            // Write changes (inserts/updates)
            for (ComparisonResult result : results) {
                if (!result.changes().isEmpty()) {
                    long fStart = System.nanoTime();
                    StringBuilder chunk = new StringBuilder();
                    chunk.append("-- TABLE ").append(result.tableName()).append("\n");
                    chunk.append(result.changes());
                    formatMs += (System.nanoTime() - fStart) / 1_000_000L;

                    long wStart = System.nanoTime();
                    out.write(chunk.toString());
                    ioMs += (System.nanoTime() - wStart) / 1_000_000L;
                    if (progressBar != null) {
                        progressBar.stepBy(result.insertCount() + result.updateCount());
                    }
                }
            }

            // Write deletions
            long fStart = System.nanoTime();
            StringBuilder delHeader = new StringBuilder();
            delHeader.append("-- DELETIONS\n");
            formatMs += (System.nanoTime() - fStart) / 1_000_000L;

            long wStart = System.nanoTime();
            out.write(delHeader.toString());
            ioMs += (System.nanoTime() - wStart) / 1_000_000L;
            for (ComparisonResult result : results) {
                if (!result.deletions().isEmpty()) {
                    long dfStart = System.nanoTime();
                    StringBuilder dchunk = new StringBuilder();
                    dchunk.append("-- TABLE ").append(result.tableName()).append("\n");
                    dchunk.append(result.deletions());
                    formatMs += (System.nanoTime() - dfStart) / 1_000_000L;

                    long dwStart = System.nanoTime();
                    out.write(dchunk.toString());
                    ioMs += (System.nanoTime() - dwStart) / 1_000_000L;
                    if (progressBar != null) {
                        progressBar.stepBy(result.deleteCount());
                    }
                }
            }

            long tfStart = System.nanoTime();
            String tail = "SET FOREIGN_KEY_CHECKS = 1;\n";
            formatMs += (System.nanoTime() - tfStart) / 1_000_000L;

            long twStart = System.nanoTime();
            out.write(tail);
            ioMs += (System.nanoTime() - twStart) / 1_000_000L;
        }
        return new WriteTiming(formatMs, ioMs);
    }

    private String sanitizeFilename(String name) {
        return name.replaceAll("[^A-Za-z0-9_.-]+", "_");
    }

    private void writeTimingJsonReport(
            String path,
            List<TableTiming> timings,
            long schemaMs,
            long splitMs,
            long compareMs,
            long writeMs,
            long writeFormatMs,
            long writeIoMs,
            long wallMs)
            throws IOException {
        List<TableTiming> sorted = new ArrayList<>(timings);
        sorted.sort(Comparator.comparingLong(TableTiming::totalMs).reversed());

        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"schema_ms\": ").append(schemaMs).append(",\n");
        sb.append("  \"split_ms\": ").append(splitMs).append(",\n");
        sb.append("  \"compare_ms\": ").append(compareMs).append(",\n");
        sb.append("  \"delete_ms\": ").append(0).append(",\n");
        sb.append("  \"write_ms\": ").append(writeMs).append(",\n");
        sb.append("  \"write_format_ms\": ").append(writeFormatMs).append(",\n");
        sb.append("  \"write_io_ms\": ").append(writeIoMs).append(",\n");
        sb.append("  \"total_ms\": ")
                .append(schemaMs + splitMs + compareMs + writeMs)
                .append(",\n");
        sb.append("  \"wall_ms\": ").append(wallMs).append(",\n");
        sb.append("  \"tables\": [\n");
        for (int i = 0; i < sorted.size(); i++) {
            TableTiming t = sorted.get(i);
            sb.append("    {");
            sb.append("\"table\": \"").append(t.tableName()).append("\", ");
            sb.append("\"load_ms\": ").append(t.loadMs()).append(", ");
            sb.append("\"compare_ms\": ").append(t.compareMs()).append(", ");
            sb.append("\"delete_ms\": ").append(t.deleteMs()).append(", ");
            sb.append("\"total_ms\": ").append(t.totalMs());
            sb.append("}");
            if (i < sorted.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append("  ]\n");
        sb.append("}\n");

        Files.writeString(Path.of(path), sb.toString());
    }

    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }
}
