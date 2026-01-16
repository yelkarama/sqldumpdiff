package com.sqldumpdiff;

import java.util.HashMap;
import java.util.Map;

public class SqliteProfilesFile {
    private Map<String, SqliteProfile> profiles = new HashMap<>();

    public SqliteProfilesFile() {
    }

    public Map<String, SqliteProfile> getProfiles() {
        return profiles;
    }

    public void setProfiles(Map<String, SqliteProfile> profiles) {
        this.profiles = profiles != null ? profiles : new HashMap<>();
    }
}
