package com.sqldumpdiff;

public record TableCompareResult(
        ComparisonResult result,
        TableTiming timing) {
}
