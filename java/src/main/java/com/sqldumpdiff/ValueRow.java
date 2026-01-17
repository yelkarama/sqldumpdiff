package com.sqldumpdiff;

import java.util.List;

/**
 * Lightweight row holder for split phase: table + columns + values.
 */
public record ValueRow(
        String table,
        List<String> columns,
        List<String> values) {
}
