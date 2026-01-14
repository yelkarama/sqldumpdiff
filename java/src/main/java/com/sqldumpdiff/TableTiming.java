package com.sqldumpdiff;

public record TableTiming(
        String tableName,
        long loadMs,
        long compareMs,
        long deleteMs) {

    public long totalMs() {
        return loadMs + compareMs + deleteMs;
    }
}
