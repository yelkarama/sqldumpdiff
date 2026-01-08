package com.sqldumpdiff;

import lombok.extern.java.Log;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.LogManager;

/**
 * High-performance SQL dump comparison tool using Java 21+ virtual threads.
 * <p>
 * Virtual threads eliminate the overhead of Python's multiprocessing and GIL,
 * allowing true parallel execution with minimal memory footprint.
 */
@Log
public class SqlDumpDiff {

    static {
        // Load logging configuration from classpath
        try (InputStream loggingConfig = SqlDumpDiff.class.getResourceAsStream("/logging.properties")) {
            if (loggingConfig != null) {
                LogManager.getLogManager().readConfiguration(loggingConfig);
                log.info("Logging configuration loaded from logging.properties");
            } else {
                System.err.println("Warning: logging.properties not found, using default configuration");
            }
        } catch (IOException e) {
            System.err.println("Warning: Failed to load logging.properties: " + e.getMessage());
        }
    }

    static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: sqldumpdiff <old_dump.sql> <new_dump.sql> [output.sql]");
            System.err.println("       If output.sql is not provided, delta SQL is printed to stdout");
            System.exit(1);
        }

        String oldFile = args[0];
        String newFile = args[1];
        String outputFile = args.length > 2 ? args[2] : null;

        try {
            Instant start = Instant.now();
            log.info("Starting SQL dump comparison");
            log.info("Old dump: " + oldFile);
            log.info("New dump: " + newFile);
            if (outputFile != null) {
                log.info("Output file: " + outputFile);
            }

            DeltaGenerator generator = new DeltaGenerator();
            generator.generateDelta(oldFile, newFile, outputFile);

            Duration duration = Duration.between(start, Instant.now());
            System.err.printf("\nCompleted in %d.%03ds\n",
                    duration.toSeconds(),
                    duration.toMillisPart());
            log.info("Comparison completed successfully in " + duration.toMillis() + "ms");

        } catch (IOException | InterruptedException | java.util.concurrent.ExecutionException e) {
            log.log(java.util.logging.Level.SEVERE, "Fatal error during comparison", e);
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
