# Consistency Checks

This repo has three implementations. To ensure outputs stay aligned, use the
following lightweight checks.

## Count‑only comparison

Compare summary counts across implementations:

```bash
./scripts/run-go.sh   old.sql new.sql /tmp/delta_go.sql  | tail -n +1
./scripts/run-java.sh old.sql new.sql /tmp/delta_java.sql | tail -n +1
./scripts/run-py.sh   old.sql new.sql /tmp/delta_py.sql  | tail -n +1
```

## Output diff

Compare full delta output:

```bash
diff -u /tmp/delta_go.sql /tmp/delta_java.sql
```

If you see differences:
1) Check the schema parser and PK detection.
2) Ensure NULL normalization matches.
3) Confirm column order handling (hashing/updates).

