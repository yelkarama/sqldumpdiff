package com.sqldumpdiff;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class SqliteProfileLoader {
    public static SqliteProfilesFile load(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            Yaml yaml = new Yaml(new Constructor(SqliteProfilesFile.class));
            return yaml.load(in);
        }
    }
}
