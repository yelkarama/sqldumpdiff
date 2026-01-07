#!/bin/bash

# Benchmark script to find optimal worker settings for sqldumpdiff
# Usage: ./benchmark.sh old.sql new.sql

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <old_dump.sql> <new_dump.sql>"
    exit 1
fi

OLD_FILE="$1"
NEW_FILE="$2"
CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

echo "=========================================="
echo "SQL Dump Diff Benchmark"
echo "=========================================="
echo "CPU Cores: $CPU_CORES"
echo "Old file: $OLD_FILE"
echo "New file: $NEW_FILE"
echo ""

# Test configurations
# Format: PARALLEL_INSERTS:INSERT_WORKERS:EXECUTOR:WORKERS
CONFIGS=(
    # Baseline: No parallel INSERT processing
    "0::process:$CPU_CORES"
    
    # Parallel INSERTs with processes (recommended)
    "1:$CPU_CORES:process:$CPU_CORES"
    "1:$((CPU_CORES * 2)):process:$CPU_CORES"
    "1:-1:process:$CPU_CORES"
    
    # Parallel INSERTs with threads (for comparison)
    "1:$CPU_CORES:thread:$CPU_CORES"
    
    # High worker counts for table comparison
    "1:$CPU_CORES:process:$((CPU_CORES * 2))"
    "1:$CPU_CORES:process:-1"
    
    # Thread-based table comparison
    "1:$CPU_CORES:process:thread:$CPU_CORES"
)

RESULTS_FILE="benchmark_results_$(date +%Y%m%d_%H%M%S).txt"

echo "Running benchmarks... (results will be saved to $RESULTS_FILE)"
echo ""

for config in "${CONFIGS[@]}"; do
    IFS=':' read -r parallel_inserts insert_workers insert_exec table_exec workers <<< "$config"
    
    # Build description
    desc="PARALLEL_INSERTS=$parallel_inserts"
    [ -n "$insert_workers" ] && desc="$desc INSERT_WORKERS=$insert_workers"
    [ -n "$insert_exec" ] && desc="$desc INSERT_EXEC=$insert_exec"
    [ -n "$table_exec" ] && desc="$desc TABLE_EXEC=$table_exec"
    [ -n "$workers" ] && desc="$desc WORKERS=$workers"
    
    echo "----------------------------------------"
    echo "Testing: $desc"
    echo "----------------------------------------"
    
    # Build environment variables
    export SQLDUMPDIFF_PARALLEL_INSERTS="$parallel_inserts"
    [ -n "$insert_workers" ] && export SQLDUMPDIFF_INSERT_WORKERS="$insert_workers"
    [ -n "$insert_exec" ] && export SQLDUMPDIFF_INSERT_EXECUTOR="$insert_exec"
    [ -n "$workers" ] && export SQLDUMPDIFF_WORKERS="$workers"
    
    # Determine table executor (if different from insert executor)
    if [ "$table_exec" = "thread" ]; then
        export SQLDUMPDIFF_EXECUTOR="thread"
    else
        export SQLDUMPDIFF_EXECUTOR="${insert_exec:-process}"
    fi
    
    # Run benchmark
    start_time=$(date +%s)
    { time uv run sqldumpdiff.py "$OLD_FILE" "$NEW_FILE" > /dev/null; } 2>&1 | tee -a "$RESULTS_FILE"
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    echo ""
    echo "Configuration: $desc" >> "$RESULTS_FILE"
    echo "Duration: ${duration}s" >> "$RESULTS_FILE"
    echo "========================================" >> "$RESULTS_FILE"
    echo ""
    
    # Clean up environment
    unset SQLDUMPDIFF_PARALLEL_INSERTS
    unset SQLDUMPDIFF_INSERT_WORKERS
    unset SQLDUMPDIFF_INSERT_EXECUTOR
    unset SQLDUMPDIFF_EXECUTOR
    unset SQLDUMPDIFF_WORKERS
    
    # Small delay between tests
    sleep 2
done

echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "Summary of timings:"
grep "Duration:" "$RESULTS_FILE" | nl
