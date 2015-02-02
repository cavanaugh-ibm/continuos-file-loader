package com.cloudant.se.loader.file.write;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.cloudant.client.api.Database;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileLoader extends BaseFileLoader {
	public JsonFileLoader(Configuration config, Database database, Path dirCompleted, Path dirFailed, Path name, Path path) {
		super(config, database, dirCompleted, dirFailed, name, path);
	}

	@Override
	protected Map<String, Object> getContentsAsMap() throws IOException {
		return new ObjectMapper().reader(Map.class).readValue(Files.readAllBytes(path));
	}
}
