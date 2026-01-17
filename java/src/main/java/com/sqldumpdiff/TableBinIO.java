package com.sqldumpdiff;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary table file codec (header + row values).
 * Format:
 *  - magic "SQDR" (4 bytes)
 *  - version u32 LE (1)
 *  - column count u32 LE
 *  - columns: [len u32 LE][bytes]...
 *  - rows: [val count u32 LE][len u32 LE][bytes]...
 */
public final class TableBinIO {
    private static final byte[] MAGIC = new byte[] { 'S', 'Q', 'D', 'R' };
    private static final int VERSION = 1;

    private TableBinIO() {}

    public static TableBinWriter openWriter(Path path, List<String> columns) throws IOException {
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(path));
        writeHeader(out, columns);
        return new TableBinWriter(out, columns);
    }

    public static TableBinReader openReader(Path path) throws IOException {
        InputStream in = new BufferedInputStream(Files.newInputStream(path));
        List<String> columns = readHeader(in);
        return new TableBinReader(in, columns);
    }

    private static void writeHeader(OutputStream out, List<String> columns) throws IOException {
        out.write(MAGIC);
        writeU32(out, VERSION);
        writeU32(out, columns.size());
        for (String col : columns) {
            writeBytes(out, col.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static List<String> readHeader(InputStream in) throws IOException {
        byte[] magic = in.readNBytes(4);
        if (magic.length < 4 || magic[0] != MAGIC[0] || magic[1] != MAGIC[1] || magic[2] != MAGIC[2]
                || magic[3] != MAGIC[3]) {
            throw new IOException("invalid table file magic");
        }
        int version = readU32(in);
        if (version != VERSION) {
            throw new IOException("unsupported table file version: " + version);
        }
        int colCount = readU32(in);
        List<String> columns = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            byte[] bytes = readBytes(in);
            columns.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return columns;
    }

    private static void writeU32(OutputStream out, int value) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(value);
        out.write(buf.array());
    }

    private static int readU32(InputStream in) throws IOException {
        byte[] b = in.readNBytes(4);
        if (b.length < 4) {
            throw new EOFException();
        }
        return ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static void writeBytes(OutputStream out, byte[] bytes) throws IOException {
        writeU32(out, bytes.length);
        if (bytes.length > 0) {
            out.write(bytes);
        }
    }

    private static byte[] readBytes(InputStream in) throws IOException {
        int len = readU32(in);
        if (len < 0) {
            throw new IOException("invalid length");
        }
        if (len == 0) {
            return new byte[0];
        }
        byte[] b = in.readNBytes(len);
        if (b.length < len) {
            throw new EOFException();
        }
        return b;
    }

    public static final class TableBinWriter implements Closeable {
        private final OutputStream out;
        private final List<String> columns;

        private TableBinWriter(OutputStream out, List<String> columns) {
            this.out = out;
            this.columns = columns;
        }

        public void writeRow(java.util.Map<String, String> data) throws IOException {
            writeU32(out, columns.size());
            for (String col : columns) {
                String val = data.get(col);
                byte[] bytes = val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8);
                writeBytes(out, bytes);
            }
        }

        public void writeRowValues(List<String> values) throws IOException {
            writeU32(out, columns.size());
            for (int i = 0; i < columns.size(); i++) {
                String val = i < values.size() ? values.get(i) : null;
                byte[] bytes = val == null ? new byte[0] : val.getBytes(StandardCharsets.UTF_8);
                writeBytes(out, bytes);
            }
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    public static final class TableBinReader implements Closeable {
        private final InputStream in;
        private final List<String> columns;

        private TableBinReader(InputStream in, List<String> columns) {
            this.in = in;
            this.columns = columns;
        }

        public List<String> columns() {
            return columns;
        }

        public List<String> readRow() throws IOException {
            try {
                int count = readU32(in);
                List<String> values = new ArrayList<>(columns.size());
                int min = Math.min(count, columns.size());
                for (int i = 0; i < min; i++) {
                    byte[] bytes = readBytes(in);
                    values.add(bytes.length == 0 ? null : new String(bytes, StandardCharsets.UTF_8));
                }
                for (int i = min; i < count; i++) {
                    byte[] bytes = readBytes(in);
                    if (bytes == null) {
                        break;
                    }
                }
                while (values.size() < columns.size()) {
                    values.add(null);
                }
                return values;
            } catch (EOFException eof) {
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
