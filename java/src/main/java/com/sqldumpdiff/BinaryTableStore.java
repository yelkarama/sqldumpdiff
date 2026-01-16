package com.sqldumpdiff;

import java.io.IOException;
import java.io.EOFException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.Blake3;

/**
 * Stores table rows in a binary file under /tmp and keeps an in-memory index
 * from PK hash to file offset. This replaces the SQLite store.
 */
public class BinaryTableStore implements AutoCloseable {
    private final Path filePath;
    private final FileChannel channel;
    private final Map<String, Long> index = new HashMap<>();
    private final List<String> columns;
    private final List<String> pkColumns;

    public BinaryTableStore(String table, List<String> columns, List<String> pkColumns) throws IOException {
        this.columns = columns;
        this.pkColumns = pkColumns;
        this.filePath = Files.createTempFile("sqldumpdiff_store_" + table + "_", ".bin");
        this.channel = new RandomAccessFile(filePath.toFile(), "rw").getChannel();
    }

    public void insertRow(InsertRow row) throws IOException {
        String pkHash = hashPrimaryKey(row, pkColumns);
        byte[] rowHash = hashRow(row, columns);
        byte[] blob = encodeRowValues(row, columns);
        long offset = channel.position();
        index.put(pkHash, offset);

        ByteBuffer header = ByteBuffer.allocate(36).order(ByteOrder.LITTLE_ENDIAN);
        header.put(rowHash);
        header.putInt(blob.length);
        header.flip();
        writeFully(channel, header);
        if (blob.length > 0) {
            writeFully(channel, ByteBuffer.wrap(blob));
        }
    }

    public void executeBatch() throws IOException {
        channel.force(false);
    }

    public void analyzeForQuery() {
        // no-op for binary store
    }

    public long getRowCount() {
        return index.size();
    }

    public Set<String> getAllPkHashes() {
        return index.keySet();
    }

    public Map<String, String> getRowData(String pkHash) throws IOException {
        Long offset = index.get(pkHash);
        if (offset == null) {
            return null;
        }
        return readRowAt(offset);
    }

    public Map<String, Map<String, String>> getRowDataBatch(List<String> pkHashes) throws IOException {
        Map<String, Map<String, String>> out = new HashMap<>();
        for (String pkHash : pkHashes) {
            Map<String, String> row = getRowData(pkHash);
            if (row != null) {
                out.put(pkHash, row);
            }
        }
        return out;
    }

    private Map<String, String> readRowAt(long offset) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(36).order(ByteOrder.LITTLE_ENDIAN);
        readFully(channel, header, offset);
        header.flip();
        byte[] rowHash = new byte[32];
        header.get(rowHash);
        int len = header.getInt();
        ByteBuffer buf = ByteBuffer.allocate(len);
        if (len > 0) {
            readFully(channel, buf, offset + 36);
        }
        buf.flip();
        byte[] blob = new byte[len];
        if (len > 0) {
            buf.get(blob);
        }
        Map<String, String> data = decodeRowValues(blob, columns);
        return data;
    }

    private static void readFully(FileChannel ch, ByteBuffer buf, long offset) throws IOException {
        long pos = offset;
        while (buf.hasRemaining()) {
            int read = ch.read(buf, pos);
            if (read < 0) {
                break;
            }
            pos += read;
        }
        if (buf.hasRemaining()) {
            throw new IOException("row blob invalid length");
        }
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static byte[] encodeRowValues(InsertRow row, List<String> columns) throws IOException {
        int capacity = 4;
        for (String col : columns) {
            String val = row.data().get(col);
            byte[] bytes = val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8);
            capacity += 4 + bytes.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(columns.size());
        for (String col : columns) {
            String val = row.data().get(col);
            byte[] bytes = val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8);
            buf.putInt(bytes.length);
            if (bytes.length > 0) {
                buf.put(bytes);
            }
        }
        return buf.array();
    }

    private static Map<String, String> decodeRowValues(byte[] buf, List<String> columns) throws IOException {
        if (buf.length < 4) {
            throw new IOException("row blob too small");
        }
        int pos = 0;
        int count = readIntLE(buf, pos);
        pos += 4;
        Map<String, String> data = new HashMap<>(columns.size());
        int min = Math.min(count, columns.size());
        for (int i = 0; i < min; i++) {
            if (pos + 4 > buf.length) {
                throw new IOException("row blob truncated");
            }
            int len = readIntLE(buf, pos);
            pos += 4;
            if (len < 0 || pos + len > buf.length) {
                throw new IOException("row blob invalid length");
            }
            byte[] b = new byte[len];
            if (len > 0) {
                System.arraycopy(buf, pos, b, 0, len);
                pos += len;
            }
            data.put(columns.get(i), len == 0 ? null : new String(b, StandardCharsets.UTF_8));
        }
        for (int i = min; i < count; i++) {
            if (pos + 4 > buf.length) {
                break;
            }
            int len = readIntLE(buf, pos);
            pos += 4;
            if (len < 0 || pos + len > buf.length) {
                break;
            }
            pos += len;
        }
        for (int i = data.size(); i < columns.size(); i++) {
            data.put(columns.get(i), null);
        }
        return data;
    }

    private static int readIntLE(byte[] buf, int pos) {
        return ((buf[pos + 3] & 0xFF) << 24)
                | ((buf[pos + 2] & 0xFF) << 16)
                | ((buf[pos + 1] & 0xFF) << 8)
                | (buf[pos] & 0xFF);
    }

    private static String hashPrimaryKey(InsertRow row, List<String> pkColumns) {
        StringBuilder sb = new StringBuilder();
        for (String col : pkColumns) {
            String val = row.data().get(col);
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return blake3Hex(sb.toString());
    }

    public static String hashPrimaryKeyValues(List<String> pkValues) {
        StringBuilder sb = new StringBuilder();
        for (String val : pkValues) {
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return blake3Hex(sb.toString());
    }

    private static byte[] hashRow(InsertRow row, List<String> columns) {
        Blake3 hasher = Blake3.initHash();
        for (String col : columns) {
            hasher.update(col.toLowerCase().getBytes(StandardCharsets.UTF_8));
            hasher.update("=".getBytes(StandardCharsets.UTF_8));
            String val = row.data().get(col);
            String norm = normalizeNull(val);
            if (norm == null) {
                norm = "NULL";
            }
            hasher.update(norm.getBytes(StandardCharsets.UTF_8));
            hasher.update(";".getBytes(StandardCharsets.UTF_8));
        }
        return hasher.doFinalize(32);
    }

    private static String blake3Hex(String input) {
        Blake3 hasher = Blake3.initHash();
        hasher.update(input.getBytes(StandardCharsets.UTF_8));
        byte[] hash = hasher.doFinalize(32);
        return Hex.encodeHexString(hash);
    }

    private static String normalizeNull(String val) {
        if (val == null) {
            return null;
        }
        String trimmed = val.trim();
        if ("NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }
        if (trimmed.length() >= 2 && trimmed.startsWith("'") && trimmed.endsWith("'")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
            trimmed = trimmed.replace("''", "'");
        }
        return trimmed;
    }

    @Override
    public void close() throws IOException {
        channel.close();
        Files.deleteIfExists(filePath);
    }
}
