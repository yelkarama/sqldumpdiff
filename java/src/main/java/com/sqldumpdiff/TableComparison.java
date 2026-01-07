package com.sqldumpdiff;

import java.util.List;
import java.nio.file.Path;

public record TableComparison(
        String tableName,
        List<String> pkColumns,
        Path oldFile,
        Path newFile) {
}
