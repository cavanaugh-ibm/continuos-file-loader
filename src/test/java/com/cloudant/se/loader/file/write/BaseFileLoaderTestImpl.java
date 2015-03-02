package com.cloudant.se.loader.file.write;

import java.nio.file.Path;

import com.cloudant.client.api.Database;

public class BaseFileLoaderTestImpl extends JsonFileLoader {
    public BaseFileLoaderTestImpl(Database database, Path dirCompleted, Path dirFailed, Path path, boolean idFromFilename, String... idSourceFields) {
        super(database, dirCompleted, dirFailed, path, idFromFilename, idSourceFields);
    }
}
