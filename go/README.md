# SQL Dump Diff - Go Implementation

High-performance SQL dump comparison tool written in Go with goroutines for parallel processing.

## Features

- **Fast**: Go's native concurrency with goroutines
- **Memory efficient**: Streaming parser with minimal memory footprint
- **Simple**: Easy to understand codebase with idiomatic Go
- **Portable**: Single binary with no dependencies

## Prerequisites

- Go 1.23 or later
- macOS, Linux, or Windows

## Installation

```bash
cd ~/git/sqldumpdiff/go

# Download dependencies
go mod download

# Build the binary
go build -o sqldumpdiff ./cmd/sqldumpdiff

# Optional: Install to $GOPATH/bin
go install ./cmd/sqldumpdiff
```

## Usage

```bash
# Compare two SQL dumps and write to file
./sqldumpdiff old_dump.sql new_dump.sql delta.sql

# Compare and output to stdout
./sqldumpdiff old_dump.sql new_dump.sql
```

## SQLite Tuning Flags

These flags let you trade memory for speed. Defaults reflect the current tuned values.

```bash
# Adjust SQLite memory usage and concurrency
./sqldumpdiff \
  --sqlite-cache-kb 800000 \
  --sqlite-mmap-mb 128 \
  --sqlite-batch 20000 \
  --sqlite-workers 0 \
  old_dump.sql new_dump.sql delta.sql
```

Flag details:
- `--sqlite-profile`: `low-mem`, `balanced`, or `fast` (applies a preset, then per-flag overrides apply).
- `--sqlite-cache-kb`: SQLite page cache size in KB (negative means KB). Lower it to reduce RSS.
- `--sqlite-mmap-mb`: SQLite mmap size in MB. Lower to reduce RSS.
- `--sqlite-batch`: Rows per insert batch. Higher can improve speed at the cost of memory.
- `--sqlite-workers`: Max concurrent table comparisons. `0` uses `NumCPU`.

## Project Structure

```
go/
├── cmd/
│   └── sqldumpdiff/      # Main application entry point
│       └── main.go
├── internal/             # Internal packages (not importable)
│   ├── parser/           # SQL parsing logic
│   │   ├── schema.go     # Schema and DDL parsing
│   │   └── insert.go     # INSERT statement parsing
│   ├── comparer/         # Comparison logic
│   │   ├── delta.go      # Delta generation
│   │   └── writer.go     # Output writer
│   └── store/            # Data storage
│       └── memory.go     # In-memory row store
├── go.mod                # Go module definition
└── README.md
```

## Building for Production

```bash
# Build with optimizations
go build -ldflags="-s -w" -o sqldumpdiff ./cmd/sqldumpdiff

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 go build -o sqldumpdiff-linux ./cmd/sqldumpdiff

# Cross-compile for Windows
GOOS=windows GOARCH=amd64 go build -o sqldumpdiff.exe ./cmd/sqldumpdiff
```

## Performance Tips

1. **Large files**: Go's streaming parser handles large files efficiently
2. **Memory**: Uses minimal memory due to streaming design
3. **CPU**: Scales with available CPU cores for parallel processing

## Development

```bash
# Run tests
go test ./...

# Format code
go fmt ./...

# Vet code
go vet ./...

# Run with race detector
go run -race ./cmd/sqldumpdiff old.sql new.sql
```

## Learning Go

If you're new to Go, here are key concepts used in this project:

1. **Packages**: Code organization (`package main`, `package parser`)
2. **Structs**: Data structures (`type InsertRow struct {...}`)
3. **Methods**: Functions on types (`func (ip *InsertParser) Parse()`)
4. **Interfaces**: Contracts (`io.Writer`, `io.Reader`)
5. **Goroutines**: Lightweight threads (future enhancement)
6. **Defer**: Cleanup (`defer file.Close()`)
7. **Error handling**: Explicit error returns

## Next Steps

Future enhancements:

- [ ] Add goroutine-based parallel table processing
- [ ] Implement SQLite-backed storage for very large datasets
- [ ] Add progress bars using progressbar library
- [ ] Add comprehensive tests
- [ ] Add benchmarks comparing with Java/Python versions

## License

Same as parent project
