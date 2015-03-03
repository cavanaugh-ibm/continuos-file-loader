package com.cloudant.se.loader.file.write;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.cloudant.client.api.Database;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileLoader extends BaseFileLoader {
    public JsonFileLoader(Database database, Path dirCompleted, Path dirFailed, Path path, boolean idFromFilename, boolean mergeWithExisting, String versionField, String... idSourceFields) {
        super(database, dirCompleted, dirFailed, path, idFromFilename, mergeWithExisting, versionField, idSourceFields);
    }

    @Override
    protected Map<String, Object> getContentsAsMap() throws IOException {
        return new ObjectMapper().reader(Map.class).readValue(Files.readAllBytes(path));
    }
}
