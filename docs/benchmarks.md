# Benchmarks

This repo includes multiple implementations, so results depend heavily on:

- File sizes and schema complexity
- SQLite settings (cache, mmap, batch size)
- CPU cores and I/O speed
- JVM flags (Java)

## Tips

- Run each implementation with the same inputs and output path.
- Warm up JVM before timing Java runs.
- Prefer SSD and disable antivirus on temp dirs for large files.

## Suggested Commands

Go:
```bash
cd go
./sqldumpdiff --sqlite-profile fast old.sql new.sql /tmp/delta_go.sql
```

Java:
```bash
cd java
java --enable-native-access=ALL-UNNAMED -Xmx4g -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql /tmp/delta_java.sql
```

Python:
```bash
cd python
python3 sqldumpdiff.py old.sql new.sql /tmp/delta_py.sql
```

