package com.sqldumpdiff;

import java.util.Map;

public record SqliteProfilesFile(Map<String, SqliteProfile> profiles) {
}
