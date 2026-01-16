package com.sqldumpdiff;

public class SqliteProfile {
    private int cache_kb;
    private int mmap_mb;
    private int batch;
    private int workers;

    public SqliteProfile() {
    }

    public int cache_kb() {
        return cache_kb;
    }

    public void setCache_kb(int cache_kb) {
        this.cache_kb = cache_kb;
    }

    public int mmap_mb() {
        return mmap_mb;
    }

    public void setMmap_mb(int mmap_mb) {
        this.mmap_mb = mmap_mb;
    }

    public int batch() {
        return batch;
    }

    public void setBatch(int batch) {
        this.batch = batch;
    }

    public int workers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }
}
