package com.sqldumpdiff;

public record ComparisonResult(
        String tableName,
        String changes,
        String deletions,
        int insertCount,
        int updateCount,
        int deleteCount) {
}
