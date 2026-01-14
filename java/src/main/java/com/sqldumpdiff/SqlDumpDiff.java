package com.sqldumpdiff;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.LogManager;
import java.util.stream.Stream;

import lombok.extern.java.Log;

/**
 * High-performance SQL dump comparison tool using Java 21+ virtual threads.
 * <p>
 * Virtual threads eliminate the overhead of Python's multiprocessing and GIL,
 * allowing true parallel execution with minimal memory footprint.
 */
@Log
public class SqlDumpDiff {

    static {
        // Load logging configuration from classpath but don't log yet
        try (InputStream loggingConfig = SqlDumpDiff.class.getResourceAsStream("/logging.properties")) {
            if (loggingConfig != null) {
                LogManager.getLogManager().readConfiguration(loggingConfig);
            } else {
                System.err.println("Warning: logging.properties not found, using default configuration");
            }
        } catch (IOException e) {
            System.err.println("Warning: Failed to load logging.properties: " + e.getMessage());
        }
    }

    private static void configureLogging(boolean debug) {
        java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");

        if (!debug) {
            // Remove console handler when not in debug mode - logs only go to file
            for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
                if (handler instanceof java.util.logging.ConsoleHandler) {
                    rootLogger.removeHandler(handler);
                }
            }
        } else {
            // Enable FINE level logging in debug mode for detailed output
            java.util.logging.Logger sqldumpdiffLogger = java.util.logging.Logger.getLogger("com.sqldumpdiff");
            sqldumpdiffLogger.setLevel(java.util.logging.Level.FINE);

            // Set all handlers to FINE level too
            for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
                if (handler instanceof java.util.logging.ConsoleHandler) {
                    handler.setLevel(java.util.logging.Level.FINE);
                }
            }
        }

        // Now log the configuration loading
        log.info("Logging configuration loaded - debug mode: " + debug);
    }

    static void main(String[] args) {
        Instant wallStart = Instant.now();
        // Parse flags first (supports --debug, --timing, --timing-json in any order).
        boolean debug = false;
        boolean timing = false;
        String timingJson = null;
        java.util.List<String> positional = new java.util.ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--debug".equals(arg)) {
                debug = true;
                continue;
            }
            if ("--timing".equals(arg)) {
                timing = true;
                continue;
            }
            if (arg.startsWith("--timing-json")) {
                if (arg.contains("=")) {
                    timingJson = arg.substring(arg.indexOf('=') + 1);
                } else if (i + 1 < args.length) {
                    timingJson = args[++i];
                }
                continue;
            }
            positional.add(arg);
        }

        // Configure logging based on debug flag (before any logging happens)
        configureLogging(debug);

        if (positional.size() < 2) {
            System.err.println("Usage: sqldumpdiff [--debug] [--timing] [--timing-json <file>] <old_dump.sql> <new_dump.sql> [output.sql]");
            System.err.println();
            System.err.println("Options:");
            System.err.println("  --debug        Enable detailed console logging and disable progress bars");
            System.err.println("  --timing       Emit timing diagnostics even without --debug");
            System.err.println("  --timing-json  Write timing report JSON to the given file");
            System.err.println();
            System.err.println("Notes:");
            System.err.println("  If output.sql is not provided, delta SQL is printed to stdout");
            System.exit(1);
        }

        String oldFile = positional.get(0);
        String newFile = positional.get(1);
        String outputFile = positional.size() > 2 ? positional.get(2) : null;

        try {
            Instant start = Instant.now();
            log.info("Starting SQL dump comparison");
            log.log(java.util.logging.Level.INFO, "Old dump: {0}", oldFile);
            log.log(java.util.logging.Level.INFO, "New dump: {0}", newFile);
            if (outputFile != null) {
                log.log(java.util.logging.Level.INFO, "Output file: {0}", outputFile);
            }

            DeltaGenerator generator = new DeltaGenerator();
            generator.generateDelta(oldFile, newFile, outputFile, debug, timing, timingJson, start, wallStart);

            Duration duration = Duration.between(start, Instant.now());
            System.err.printf("\nCompleted in %d.%03ds\n",
                    duration.toSeconds(),
                    duration.toMillisPart());

            // Flush all logging handlers to ensure file is written
            java.util.logging.Logger rootLogger = java.util.logging.Logger.getLogger("");
            for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
                handler.flush();
            }

            // Find and print the actual logfile only in non-debug mode
            if (!debug) {
                try (Stream<java.nio.file.Path> paths = Files.list(Paths.get("/tmp"))) {
                    java.nio.file.Path logFile = paths
                            .filter(p -> p.getFileName().toString().startsWith("sqldumpdiff"))
                            .max((a, b) -> Long.compare(a.toFile().lastModified(), b.toFile().lastModified()))
                            .orElse(null);
                    if (logFile != null) {
                        System.err.println("Log file: " + logFile);
                    }
                } catch (IOException ignored) {
                    // If we can't find the file, that's okay
                }
            }

            log.log(java.util.logging.Level.INFO, "Comparison completed successfully in {0}ms", duration.toMillis());

        } catch (IOException | InterruptedException | java.util.concurrent.ExecutionException e) {
            log.log(java.util.logging.Level.SEVERE, "Fatal error during comparison", e);
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
