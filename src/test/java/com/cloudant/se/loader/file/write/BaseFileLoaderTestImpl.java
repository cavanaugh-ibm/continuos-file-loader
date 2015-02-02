package com.cloudant.se.loader.file.write;

import java.nio.file.Path;

import org.apache.commons.configuration.Configuration;

import com.cloudant.client.api.Database;

public class BaseFileLoaderTestImpl extends JsonFileLoader {
	public BaseFileLoaderTestImpl(Configuration config, Database database, Path dirCompleted, Path dirFailed, Path name, Path path) {
		super(config, database, dirCompleted, dirFailed, name, path);
	}
}
