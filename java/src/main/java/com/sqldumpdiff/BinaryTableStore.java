package com.sqldumpdiff;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stores table rows in a binary file under /tmp and keeps an in-memory index
 * from PK hash to file offset. This replaces the SQLite store.
 */
public class BinaryTableStore implements AutoCloseable {
    private static final int WRITE_BUFFER_BYTES = 4 * 1024 * 1024;
    private static final ThreadLocal<StringBuilder> TL_SB = ThreadLocal.withInitial(StringBuilder::new);
    private static final ThreadLocal<java.io.ByteArrayOutputStream> TL_BAOS = ThreadLocal
            .withInitial(() -> new java.io.ByteArrayOutputStream(256));
    private final Path filePath;
    private final FileChannel channel;
    private MappedByteBuffer mmap;
    private final ByteBuffer writeBuffer;
    private long writeBufStart;
    private long writePos;
    private final Map<String, Long> index = new HashMap<>();
    private final List<String> columns;
    private final List<String> pkColumns;

    public BinaryTableStore(String table, List<String> columns, List<String> pkColumns) throws IOException {
        this.columns = columns;
        this.pkColumns = pkColumns;
        this.filePath = Files.createTempFile("sqldumpdiff_store_" + table + "_", ".bin");
        RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = raf.getChannel();
        this.writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_BYTES).order(ByteOrder.LITTLE_ENDIAN);
        this.writeBufStart = 0L;
        this.writePos = 0L;
    }

    public void insertRow(InsertRow row) throws IOException {
        String pkHash = hashPrimaryKey(row, pkColumns);
        byte[] rowHash = hashRow(row, columns);
        byte[] blob = encodeRowValues(row, columns);
        long offset = writePos;
        index.put(pkHash, offset);

        int recordSize = 36 + blob.length;
        if (recordSize > writeBuffer.capacity()) {
            flushBuffer();
            ByteBuffer header = ByteBuffer.allocate(36).order(ByteOrder.LITTLE_ENDIAN);
            header.put(rowHash);
            header.putInt(blob.length);
            header.flip();
            writeFully(channel, header, writePos);
            if (blob.length > 0) {
                writeFully(channel, ByteBuffer.wrap(blob), writePos + 36);
            }
            writePos += recordSize;
            return;
        }
        if (writeBuffer.remaining() < recordSize) {
            flushBuffer();
        }
        writeBuffer.put(rowHash);
        writeBuffer.putInt(blob.length);
        if (blob.length > 0) {
            writeBuffer.put(blob);
        }
        writePos += recordSize;
    }

    public void executeBatch() throws IOException {
        flushBuffer();
        channel.force(false);
    }

    public void analyzeForQuery() {
        // no-op for binary store
    }

    public void enableMmap() {
        try {
            flushBuffer();
        } catch (IOException ignored) {
        }
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            return;
        }
        try {
            long size = channel.size();
            if (size > 0 && size <= Integer.MAX_VALUE) {
                mmap = channel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            }
        } catch (IOException ignored) {
            mmap = null;
        }
    }

    public long getRowCount() {
        return index.size();
    }

    public Set<String> getAllPkHashes() {
        return index.keySet();
    }

    public List<String> getColumns() {
        return columns;
    }

    public String[] getRowData(String pkHash) throws IOException {
        Long offset = index.get(pkHash);
        if (offset == null) {
            return null;
        }
        return readRowAt(offset);
    }

    public Map<String, String[]> getRowDataBatch(List<String> pkHashes) throws IOException {
        Map<String, String[]> out = new HashMap<>();
        for (String pkHash : pkHashes) {
            String[] row = getRowData(pkHash);
            if (row != null) {
                out.put(pkHash, row);
            }
        }
        return out;
    }

    private String[] readRowAt(long offset) throws IOException {
        if (mmap != null) {
            if (offset + 36 > mmap.limit()) {
                throw new IOException("row blob invalid length");
            }
            for (int i = 0; i < 32; i++) {
                mmap.get((int) offset + i);
            }
            int len = readIntLE(mmap, (int) offset + 32);
            int blobStart = (int) offset + 36;
            int blobEnd = blobStart + len;
            if (len < 0 || blobEnd > mmap.limit()) {
                throw new IOException("row blob invalid length");
            }
            byte[] blob = new byte[len];
            if (len > 0) {
                for (int i = 0; i < len; i++) {
                    blob[i] = mmap.get(blobStart + i);
                }
            }
            return decodeRowValues(blob, columns);
        }
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
        return decodeRowValues(blob, columns);
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

    private static void writeFully(FileChannel ch, ByteBuffer buf, long offset) throws IOException {
        long pos = offset;
        while (buf.hasRemaining()) {
            int written = ch.write(buf, pos);
            pos += written;
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

    private static String[] decodeRowValues(byte[] buf, List<String> columns) throws IOException {
        if (buf.length < 4) {
            throw new IOException("row blob too small");
        }
        int pos = 0;
        int count = readIntLE(buf, pos);
        pos += 4;
        String[] data = new String[columns.size()];
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
            data[i] = len == 0 ? null : new String(b, StandardCharsets.UTF_8);
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
        return data;
    }

    private static int readIntLE(byte[] buf, int pos) {
        return ((buf[pos + 3] & 0xFF) << 24)
                | ((buf[pos + 2] & 0xFF) << 16)
                | ((buf[pos + 1] & 0xFF) << 8)
                | (buf[pos] & 0xFF);
    }

    private static int readIntLE(MappedByteBuffer buf, int pos) {
        return ((buf.get(pos + 3) & 0xFF) << 24)
                | ((buf.get(pos + 2) & 0xFF) << 16)
                | ((buf.get(pos + 1) & 0xFF) << 8)
                | (buf.get(pos) & 0xFF);
    }

    private static String hashPrimaryKey(InsertRow row, List<String> pkColumns) {
        StringBuilder sb = TL_SB.get();
        sb.setLength(0);
        for (String col : pkColumns) {
            String val = row.data().get(col);
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return blake3Hex(sb.toString());
    }

    public static String hashPrimaryKeyValues(List<String> pkValues) {
        StringBuilder sb = TL_SB.get();
        sb.setLength(0);
        for (String val : pkValues) {
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return blake3Hex(sb.toString());
    }

    private static byte[] hashRow(InsertRow row, List<String> columns) {
        java.io.ByteArrayOutputStream out = TL_BAOS.get();
        out.reset();
        for (String col : columns) {
            out.writeBytes(col.toLowerCase().getBytes(StandardCharsets.UTF_8));
            out.writeBytes("=".getBytes(StandardCharsets.UTF_8));
            String val = row.data().get(col);
            String norm = normalizeNull(val);
            if (norm == null) {
                norm = "NULL";
            }
            out.writeBytes(norm.getBytes(StandardCharsets.UTF_8));
            out.writeBytes(";".getBytes(StandardCharsets.UTF_8));
        }
        return Blake3Hasher.hashBytes(out.toByteArray());
    }

    private static String blake3Hex(String input) {
        return Blake3Hasher.hashHex(input);
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
        flushBuffer();
        channel.close();
        Files.deleteIfExists(filePath);
    }

    private void flushBuffer() throws IOException {
        if (writeBuffer.position() == 0) {
            return;
        }
        writeBuffer.flip();
        writeFully(channel, writeBuffer, writeBufStart);
        writeBufStart = writePos;
        writeBuffer.clear();
    }
}
