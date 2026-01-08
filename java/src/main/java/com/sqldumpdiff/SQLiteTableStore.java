package com.sqldumpdiff;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Stores table rows in SQLite for memory-efficient comparison.
 * Instead of loading millions of rows into HashMaps, we use an indexed
 * SQLite database that scales to any file size.
 */
public class SQLiteTableStore {
    private final Connection connection;
    private final Path dbFile;
    private final String tableName;
    private final List<String> pkColumns;
    private PreparedStatement insertStmt;
    private PreparedStatement selectStmt;
    private PreparedStatement deleteStmt;

    public SQLiteTableStore(String table, List<String> pkColumns) throws SQLException, IOException {
        this.tableName = table;
        this.pkColumns = pkColumns;
        this.dbFile = Files.createTempFile("sqldumpdiff_", ".db");

        // SQLite connection on temp file
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath());

        // Run PRAGMAs outside a transaction (SQLite complains otherwise)
        this.connection.setAutoCommit(true);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=OFF");
            stmt.execute("PRAGMA synchronous=OFF");
            stmt.execute("PRAGMA cache_size=-600000"); // ~600MB cache if available
            stmt.execute("PRAGMA page_size=32768"); // Bigger pages reduce I/O
            stmt.execute("PRAGMA temp_store=MEMORY");
            stmt.execute("PRAGMA mmap_size=134217728"); // 128MB mmap window
            stmt.execute("PRAGMA busy_timeout=5000");
        }

        // Now switch to manual commits for batched inserts
        this.connection.setAutoCommit(false);

        createTable();
        prepareStatements();
    }

    private void createTable() throws SQLException {
        // Create table with all columns as text (simpler for dynamic schema)
        String sql = "CREATE TABLE rows (pk_hash TEXT PRIMARY KEY, data TEXT)";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void prepareStatements() throws SQLException {
        String insertSql = "INSERT OR REPLACE INTO rows (pk_hash, data) VALUES (?, ?)";
        insertStmt = connection.prepareStatement(insertSql);

        String selectSql = "SELECT data FROM rows WHERE pk_hash = ?";
        selectStmt = connection.prepareStatement(selectSql);

        String deleteSql = "DELETE FROM rows WHERE pk_hash = ?";
        deleteStmt = connection.prepareStatement(deleteSql);
    }

    public void insertRow(InsertRow row) throws SQLException {
        String pkHash = hashPrimaryKey(row, pkColumns);
        String json = row.toJson();
        insertStmt.setString(1, pkHash);
        insertStmt.setString(2, json);
        insertStmt.addBatch();
    }

    public void executeBatch() throws SQLException {
        if (insertStmt != null) {
            insertStmt.executeBatch();
            insertStmt.clearBatch();
            connection.commit();
        }
    }

    public void analyzeForQuery() throws SQLException {
        // Update statistics for query planner after all inserts
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("ANALYZE");
        }
    }

    public Map<String, String> getRowData(String pkHash) throws SQLException {
        selectStmt.setString(1, pkHash);
        try (ResultSet rs = selectStmt.executeQuery()) {
            if (rs.next()) {
                String json = rs.getString("data");
                InsertRow row = InsertRow.fromJson(json, tableName);
                return row.data();
            }
        }
        return null;
    }

    public Set<String> getAllPkHashes() throws SQLException {
        Set<String> hashes = new HashSet<>();
        String sql = "SELECT pk_hash FROM rows";
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                hashes.add(rs.getString("pk_hash"));
            }
        }
        return hashes;
    }

    public long getRowCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM rows";
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    public void close() throws SQLException {
        if (insertStmt != null) {
            insertStmt.close();
        }
        if (selectStmt != null) {
            selectStmt.close();
        }
        if (deleteStmt != null) {
            deleteStmt.close();
        }
        if (connection != null) {
            connection.commit();
            connection.close();
        }
        // Clean up temp database
        try {
            Files.deleteIfExists(dbFile);
            Files.deleteIfExists(Paths.get(dbFile + "-shm"));
            Files.deleteIfExists(Paths.get(dbFile + "-wal"));
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }

    private String hashPrimaryKey(InsertRow row, List<String> pkColumns) {
        // Use SHA256 hash of PK values to avoid collisions
        // This keeps the key small even with large PK values
        StringBuilder sb = new StringBuilder();
        for (String col : pkColumns) {
            String val = row.data().get(col);
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return sha256(sb.toString());
    }

    public static String hashPrimaryKeyValues(List<String> pkValues) {
        // Use SHA256 hash of PK values
        StringBuilder sb = new StringBuilder();
        for (String val : pkValues) {
            sb.append(val == null ? "NULL" : val).append("|");
        }
        return sha256(sb.toString());
    }

    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    });

    private static String sha256(String input) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        byte[] hash = digest.digest(input.getBytes());
        StringBuilder hexString = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1)
                hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
