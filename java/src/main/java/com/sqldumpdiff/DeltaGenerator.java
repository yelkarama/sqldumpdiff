package com.sqldumpdiff;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
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

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * Main delta generation logic using virtual threads for maximum concurrency.
 */
public class DeltaGenerator {

    public void generateDelta(String oldFilePath, String newFilePath, String outputPath)
            throws IOException, InterruptedException, ExecutionException {

        Path oldFile = Path.of(oldFilePath);
        Path newFile = Path.of(newFilePath);

        if (!Files.exists(oldFile)) {
            throw new FileNotFoundException("Old file not found: " + oldFilePath);
        }
        if (!Files.exists(newFile)) {
            throw new FileNotFoundException("New file not found: " + newFilePath);
        }

        System.err.println("Step 1: Parsing schemas from DDL...");

        SchemaParser schemaParser = new SchemaParser();
        Map<String, List<String>> pkMap;
        Map<String, List<String>> oldColumnsMap;
        Map<String, List<String>> newColumnsMap;
        
        try (ProgressBar pb = new ProgressBarBuilder()
                .setTaskName("Parsing schemas")
                .setInitialMax(3)
                .setStyle(ProgressBarStyle.ASCII)
                .build()) {
            pkMap = schemaParser.parseSchemas(newFile);
            pb.step();
            oldColumnsMap = schemaParser.parseColumns(oldFile);
            pb.step();
            newColumnsMap = schemaParser.parseColumns(newFile);
            pb.step();
        }

        if (pkMap.isEmpty()) {
            System.err.println("Warning: No tables with PRIMARY KEY found in new file schema.");
        }

        System.err.println("Step 2: Splitting dumps by table using virtual threads...");

        Path tempDir = Files.createTempDirectory("sqldumpdiff_");
        try {
            // Split both files in parallel using virtual threads
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                long oldFileSize = Files.size(oldFile);
                long newFileSize = Files.size(newFile);
                
                ProgressBar oldPb = new ProgressBarBuilder()
                        .setTaskName("Splitting old dump")
                        .setInitialMax(oldFileSize)
                        .setStyle(ProgressBarStyle.ASCII)
                        .setUnit(" bytes", 1)
                        .build();
                        
                ProgressBar newPb = new ProgressBarBuilder()
                        .setTaskName("Splitting new dump")
                        .setInitialMax(newFileSize)
                        .setStyle(ProgressBarStyle.ASCII)
                        .setUnit(" bytes", 1)
                        .build();
                
                Future<Map<String, Path>> oldFuture = executor
                        .submit(() -> splitDumpByTable(oldFile, oldColumnsMap, pkMap, tempDir, "old", oldPb));
                Future<Map<String, Path>> newFuture = executor
                        .submit(() -> splitDumpByTable(newFile, newColumnsMap, pkMap, tempDir, "new", newPb));

                Map<String, Path> oldTableFiles = oldFuture.get();
                Map<String, Path> newTableFiles = newFuture.get();
                
                oldPb.close();
                newPb.close();

                Set<String> allTables = new HashSet<>();
                allTables.addAll(oldTableFiles.keySet());
                allTables.addAll(newTableFiles.keySet());

                if (allTables.isEmpty()) {
                    System.err.println("No tables with primary keys found to process.");
                    return;
                }

                System.err.printf("Step 3: Comparing %d tables using virtual threads...\n", allTables.size());

                // Process all tables in parallel with virtual threads
                List<TableComparison> comparisons = allTables.stream()
                        .map(table -> new TableComparison(
                                table,
                                pkMap.get(table),
                                oldTableFiles.get(table),
                                newTableFiles.get(table)))
                        .toList();

                List<ComparisonResult> results = new ArrayList<>();
                try (ProgressBar pb = new ProgressBarBuilder()
                        .setTaskName("Comparing tables")
                        .setInitialMax(comparisons.size())
                        .setStyle(ProgressBarStyle.ASCII)
                        .build();
                     ExecutorService compareExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
                    
                    List<Future<ComparisonResult>> futures = comparisons.stream()
                            .map(comp -> compareExecutor.submit(() -> {
                                ComparisonResult result = compareTable(comp);
                                pb.step();
                                return result;
                            }))
                            .toList();

                    for (Future<ComparisonResult> future : futures) {
                        results.add(future.get());
                    }
                }

                System.err.println("Step 4: Writing delta script...");
                
                int totalChanges = results.stream()
                        .mapToInt(r -> r.insertCount() + r.updateCount() + r.deleteCount())
                        .sum();
                
                try (ProgressBar pb = new ProgressBarBuilder()
                        .setTaskName("Writing delta SQL")
                        .setInitialMax(Math.max(totalChanges, 1))
                        .setStyle(ProgressBarStyle.ASCII)
                        .build()) {
                    writeDeltaScript(results, outputPath, pb);
                }

                // Print summary
                int totalInserts = results.stream().mapToInt(ComparisonResult::insertCount).sum();
                int totalUpdates = results.stream().mapToInt(ComparisonResult::updateCount).sum();
                int totalDeletes = results.stream().mapToInt(ComparisonResult::deleteCount).sum();

                System.err.println("\n" + "=".repeat(60));
                System.err.println("SUMMARY");
                System.err.println("=".repeat(60));
                System.err.printf("Inserts:  %,d\n", totalInserts);
                System.err.printf("Updates:  %,d\n", totalUpdates);
                System.err.printf("Deletes:  %,d\n", totalDeletes);
                System.err.printf("Total:    %,d\n", totalInserts + totalUpdates + totalDeletes);
                System.err.println("=".repeat(60));
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
        Map<String, BufferedWriter> writers = new HashMap<>();
        Map<String, Path> tablePaths = new HashMap<>();
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
                        Path tablePath = tempDir.resolve(label + "_" + sanitizeFilename(row.table()) + ".jsonl");
                        tablePaths.put(row.table(), tablePath);
                        writers.put(row.table(), Files.newBufferedWriter(tablePath));
                    }

                    String json = row.toJson();
                    writers.get(row.table()).write(json);
                    writers.get(row.table()).newLine();
                }
            }
        } finally {
            for (BufferedWriter writer : writers.values()) {
                writer.close();
            }
        }

        return tablePaths;
    }

    private ComparisonResult compareTable(TableComparison comparison) {
        try {
            TableComparer comparer = new TableComparer();
            return comparer.compare(comparison);
        } catch (IOException | SQLException e) {
            System.err.println("Error comparing table " + comparison.tableName() + ": " + e.getMessage());
            return new ComparisonResult(comparison.tableName(), "", "", 0, 0, 0);
        }
    }

    private void writeDeltaScript(List<ComparisonResult> results, String outputPath, ProgressBar progressBar) throws IOException {
        Writer out = outputPath != null
                ? Files.newBufferedWriter(Path.of(outputPath))
                : new OutputStreamWriter(System.out);

        try {
            out.write("-- Full Delta Update Script\n");
            out.write("SET FOREIGN_KEY_CHECKS = 0;\n\n");

            // Sort by table name for deterministic output
            results.sort(Comparator.comparing(ComparisonResult::tableName));

            // Write changes (inserts/updates)
            for (ComparisonResult result : results) {
                if (!result.changes().isEmpty()) {
                    out.write("-- TABLE " + result.tableName() + "\n");
                    out.write(result.changes());
                    if (progressBar != null) {
                        progressBar.stepBy(result.insertCount() + result.updateCount());
                    }
                }
            }

            // Write deletions
            out.write("-- DELETIONS\n");
            for (ComparisonResult result : results) {
                if (!result.deletions().isEmpty()) {
                    out.write("-- TABLE " + result.tableName() + "\n");
                    out.write(result.deletions());
                    if (progressBar != null) {
                        progressBar.stepBy(result.deleteCount());
                    }
                }
            }

            out.write("SET FOREIGN_KEY_CHECKS = 1;\n");

        } finally {
            if (outputPath != null) {
                out.close();
            }
        }
    }

    private String sanitizeFilename(String name) {
        return name.replaceAll("[^A-Za-z0-9_.-]+", "_");
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
