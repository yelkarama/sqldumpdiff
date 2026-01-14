package com.sqldumpdiff;

public record SqliteProfile(
        int cache_kb,
        int mmap_mb,
        int batch,
        int workers) {
}
