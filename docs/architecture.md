# Architecture Overview

This repo contains three **independent** codebases that solve the same problem.
Each implementation follows the same high‑level pipeline but uses language‑specific
approaches for performance and maintainability.

## Shared Pipeline

1) Parse schemas to extract PK columns and column order.  
2) Split dumps into per‑table streams (or files).  
3) Compare old/new rows and emit inserts/updates/deletes.  
4) Write summary.

## Go

- Streaming parser to avoid loading full dumps in memory.
- Per‑table JSONL split, then per‑table SQLite DB compare for speed/locality.
- Output is streamed to avoid huge buffers.

## Java

- Splits dumps into per‑table JSONL.
- Per‑table SQLite stores old rows; new rows are streamed for compare.
- Uses native SQLite via JDBC, with aggressive PRAGMAs for speed.

## Python

- Single‑file script.
- Optimized for simplicity and reasonable performance.

## Why separate codebases?

Each language has its own performance tradeoffs and ecosystem. Keeping them
independent avoids cross‑language coupling and makes it easy to benchmark or
optimize one without breaking the others.

