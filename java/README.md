# SQL Dump Diff - Java Edition

High-performance SQL dump comparison tool using **Java 21+ Virtual Threads**.

## Why Java?

This Java implementation leverages **Project Loom's virtual threads** for true parallel execution:

- ✅ **No GIL**: Unlike Python, Java has no Global Interpreter Lock
- ✅ **Virtual Threads**: Millions of lightweight threads with minimal overhead
- ✅ **True Parallelism**: CPU-intensive parsing runs on all cores simultaneously
- ✅ **Expected Performance**: **3-5x faster** than optimized Python implementation

### Performance Comparison

| Implementation             | Time (your dataset)      | Speedup  |
| -------------------------- | ------------------------ | -------- |
| Python (baseline)          | ~8:40                    | 1x       |
| Python (optimized)         | ~5:43                    | 1.5x     |
| **Java (virtual threads)** | **~2-3 min** (estimated) | **3-5x** |

## Requirements

- **Java 21** or newer (for virtual threads)
- Maven 3.8+

## Building

```bash
cd java
mvn clean package
```

This creates an executable JAR: `target/sqldumpdiff-1.0.0.jar`

## Usage

```bash
# Using Maven
mvn exec:java -Dexec.args="old.sql new.sql output.sql"

# Using the JAR directly
java -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql output.sql

# Output to stdout
java -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql
```

## How Virtual Threads Help

### Python's Challenge

```python
# Python multiprocessing overhead:
# - Process spawning ~50-100ms each
# - Memory duplication per process
# - IPC serialization overhead
# - GIL limits threading
with ProcessPoolExecutor(max_workers=8) as executor:
    # Limited parallelism, high overhead
    executor.map(process_insert, inserts)
```

### Java's Advantage

```java
// Java virtual threads:
// - Thread creation ~1μs (1000x faster!)
// - Shared memory (no duplication)
// - No GIL - true parallelism
// - Can spawn millions of threads
try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
    // Process ALL inserts in parallel
    List<Future<Result>> futures = inserts.stream()
        .map(insert -> executor.submit(() -> process(insert)))
        .toList();
}
```

### Key Differences

| Feature         | Python                   | Java Virtual Threads |
| --------------- | ------------------------ | -------------------- |
| Thread overhead | ~10MB per thread         | ~1KB per thread      |
| Creation time   | ~50ms (process)          | ~1μs                 |
| GIL             | Yes (limits parallelism) | No                   |
| Max concurrent  | ~100s                    | Millions             |
| Memory          | Duplicated per process   | Shared               |

## Architecture

The implementation mirrors the Python version but with virtual threads:

1. **Schema Parsing**: Parse PRIMARY KEYs and columns from DDL
2. **Parallel Splitting**:
   - Use virtual threads to process old/new files simultaneously
   - Each INSERT statement processed in its own virtual thread (no worker pool limits!)
3. **Parallel Comparison**: Each table compared in its own virtual thread
4. **Delta Generation**: Write consolidated output

### Example: Processing 2658 INSERT Statements

**Python (with multiprocessing):**

```
8 worker processes × 332 inserts each = ~6 minutes
(Limited by worker pool, process overhead)
```

**Java (with virtual threads):**

```
2658 virtual threads × parallel execution = ~1-2 minutes
(All inserts processed simultaneously, minimal overhead)
```

## Development

### Project Structure

```
java/
├── pom.xml                 # Maven configuration
└── src/main/java/com/sqldumpdiff/
    ├── SqlDumpDiff.java         # Main entry point
    ├── DeltaGenerator.java       # Orchestrates the diff process
    ├── SchemaParser.java         # Parses CREATE TABLE statements
    ├── InsertParser.java         # Parses multi-row INSERT statements
    ├── TableComparer.java        # Compares old vs new table data
    ├── InsertRow.java           # Data class for parsed rows
    ├── TableComparison.java     # Data class for comparison tasks
    └── ComparisonResult.java    # Data class for results
```

### Key Features

- **Records**: Modern Java records for immutable data classes
- **Pattern Matching**: Clean, readable parsing logic
- **Streams API**: Functional data processing
- **Virtual Threads**: `Executors.newVirtualThreadPerTaskExecutor()`
- **Try-with-resources**: Automatic resource cleanup

## Benchmarking

Run against your data to see actual performance:

```bash
# Python (optimized)
time SQLDUMPDIFF_PARALLEL_INSERTS=1 SQLDUMPDIFF_EXECUTOR=thread SQLDUMPDIFF_WORKERS=-1 \
  uv run sqldumpdiff.py 11.sql 22.sql > delta_py.sql

# Java
time java -jar target/sqldumpdiff-1.0.0.jar 11.sql 22.sql > delta_java.sql

# Compare outputs
diff delta_py.sql delta_java.sql
```

## Next Steps

### Profiling with Java Flight Recorder

Identify performance bottlenecks:

```bash
# Run with profiling enabled
./profile.sh 11.sql 22.sql

# This creates profiling-TIMESTAMP/ with:
# - profile.jfr (flight recorder data)
# - delta.sql (output)
```

**Analyze the results:**

1. **JDK Mission Control (GUI - Recommended)**:

   ```bash
   # Install JMC if not already installed
   brew install jmc

   # Open profiling data
   jmc profiling-*/profile.jfr
   ```

2. **Command-line analysis**:

   ```bash
   # Quick summary
   jfr summary profiling-*/profile.jfr

   # View specific events
   jfr print --events CPULoad,GarbageCollection profiling-*/profile.jfr

   # Export to JSON for custom analysis
   jfr print --json profiling-*/profile.jfr > profile.json
   ```

**What to look for:**

- CPU hotspots in `InsertParser` or regex operations
- GC pressure (if many short-lived objects)
- Thread contention (should be minimal with virtual threads)
- I/O wait times

### GraalVM Native Compilation

Compile to a native executable for **instant startup** and **no JVM required**:

```bash
# 1. Install GraalVM
brew install --cask graalvm-jdk

# 2. Install native-image tool
$JAVA_HOME/bin/gu install native-image

# 3. Build native executable
./build-native.sh
```

This produces: `target/sqldumpdiff` - a standalone binary.

**Benefits:**

- ⚡ **Instant startup**: ~1-5ms vs ~100-200ms JVM warmup
- 📦 **No JVM needed**: Single binary, easy distribution
- 💾 **Lower memory**: No JVM overhead
- 🚀 **Peak performance**: Ahead-of-time optimized

**Usage:**

```bash
# Same as JAR, but instant startup
./target/sqldumpdiff old.sql new.sql > delta.sql

# Time comparison
time java -jar target/sqldumpdiff-1.0.0.jar 11.sql 22.sql > delta1.sql
time ./target/sqldumpdiff 11.sql 22.sql > delta2.sql
```

**Expected Performance:**

- First run (JIT warmup): Similar to JAR
- Subsequent runs: 5-10% faster
- Startup: 100x faster

### Advanced Native Image Configuration

If you encounter reflection issues:

```bash
# Generate configuration with tracing agent
./profile.sh --native-config 11.sql 22.sql

# Copy generated config
cp -r profiling-*/native-config/* src/main/resources/META-INF/native-image/

# Rebuild native image
./build-native.sh
```

### Further Optimizations

If you need even more performance:

1. **Memory-Mapped Files**: For huge dumps (>10GB)

   ```java
   MappedByteBuffer buffer = FileChannel.open(path)
       .map(FileChannel.MapMode.READ_ONLY, 0, size);
   ```

2. **Custom Parser**: Replace regex with finite state machine

   - Expected gain: 20-30% faster parsing
   - Trade-off: More code complexity

3. **Parallel I/O**: Use multiple readers for large files

   ```java
   // Split file into chunks, read in parallel
   ```

4. **Profile-Guided Optimization** (PGO):

   ```bash
   # Build with PGO instrumentation
   native-image --pgo-instrument -jar target/sqldumpdiff-1.0.0.jar

   # Run on representative data to collect profile
   ./sqldumpdiff 11.sql 22.sql > /dev/null

   # Rebuild with profile data
   native-image --pgo=default.iprof -jar target/sqldumpdiff-1.0.0.jar
   ```

## Performance Tuning

### JVM Options (for JAR execution)

```bash
# For large files (increase heap)
java -Xmx8g -XX:+UseG1GC -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql

# For many small tables (optimize for throughput)
java -XX:+UseParallelGC -XX:ParallelGCThreads=8 \
     -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql

# For low-latency (minimize GC pauses)
java -XX:+UseZGC -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql
```

### Virtual Thread Tuning

Virtual threads are automatically managed, but you can tune:

```java
// In DeltaGenerator.java, adjust carrier threads if needed:
System.setProperty("jdk.virtualThreadScheduler.parallelism", "16");
System.setProperty("jdk.virtualThreadScheduler.maxPoolSize", "256");
```

## License

Same as parent project
