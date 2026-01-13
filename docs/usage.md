# Usage

Each implementation has its own CLI and README. These are quick references.

## Go

```bash
cd go
go build -o sqldumpdiff ./cmd/sqldumpdiff
./sqldumpdiff old.sql new.sql /tmp/delta.sql
```

SQLite tuning flags:

```bash
./sqldumpdiff \
  --sqlite-profile fast \
  --sqlite-cache-kb 800000 \
  --sqlite-mmap-mb 128 \
  --sqlite-batch 20000 \
  --sqlite-workers 0 \
  old.sql new.sql /tmp/delta.sql
```

## Java

```bash
cd java
mvn -q -DskipTests package
java --enable-native-access=ALL-UNNAMED -Xmx4g -jar target/sqldumpdiff-1.0.0.jar old.sql new.sql /tmp/delta.sql
```

## Python

```bash
cd python
python3 sqldumpdiff.py old.sql new.sql /tmp/delta.sql
```

