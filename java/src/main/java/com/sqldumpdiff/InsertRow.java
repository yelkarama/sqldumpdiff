package com.sqldumpdiff;

import com.google.gson.Gson;
import java.util.*;

/**
 * Represents a single row from an INSERT statement.
 */
public record InsertRow(
        String table,
        List<String> columns,
        Map<String, String> data,
        String statement) {
    private static final Gson gson = new Gson();

    public String toJson() {
        Map<String, Object> map = new HashMap<>();
        map.put("columns", columns);
        map.put("data", data);
        map.put("stmt", statement);
        return gson.toJson(map);
    }

    @SuppressWarnings("unchecked")
    public static InsertRow fromJson(String json, String table) {
        Map<String, Object> map = gson.fromJson(json, Map.class);
        return new InsertRow(
                table,
                (List<String>) map.get("columns"),
                (Map<String, String>) map.get("data"),
                (String) map.get("stmt"));
    }
}
