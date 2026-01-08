package com.sqldumpdiff;

import java.time.Duration;
import java.time.Instant;

/**
 * High-performance SQL dump comparison tool using Java 21+ virtual threads.
 * <p>
 * Virtual threads eliminate the overhead of Python's multiprocessing and GIL,
 * allowing true parallel execution with minimal memory footprint.
 */
public class SqlDumpDiff {

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

            DeltaGenerator generator = new DeltaGenerator();
            generator.generateDelta(oldFile, newFile, outputFile);

            Duration duration = Duration.between(start, Instant.now());
            System.err.printf("\nCompleted in %d.%03ds\n",
                    duration.toSeconds(),
                    duration.toMillisPart());

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
