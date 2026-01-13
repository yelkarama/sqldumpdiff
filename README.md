# SQL Dump Diff (Multi‑language Monorepo)

This repository contains **three independent implementations** of the same SQL
dump diff tool:

- `go/` – Go implementation
- `java/` – Java implementation
- `python/` – Python implementation

Each implementation is self‑contained with its own dependencies, build tools,
and README.

## Quick Links

- Go: `go/README.md`
- Java: `java/README.md`
- Python: `python/README.md`

## Repo Structure

```
.
├── go/                # Go implementation
├── java/              # Java implementation
├── python/            # Python implementation
├── docs/              # Shared documentation
├── scripts/           # Convenience scripts
└── README.md          # This file
```

## Common Tasks

See `scripts/` for convenience wrappers:

```bash
./scripts/run-go.sh   <old.sql> <new.sql> [delta.sql]
./scripts/run-java.sh <old.sql> <new.sql> [delta.sql]
./scripts/run-py.sh   <old.sql> <new.sql> [delta.sql]
```

## Documentation

- `docs/architecture.md` – High‑level architecture for each implementation
- `docs/usage.md` – Usage examples and flags
- `docs/benchmarks.md` – Performance notes and comparison tips

