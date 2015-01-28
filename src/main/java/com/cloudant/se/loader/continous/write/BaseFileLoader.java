package com.cloudant.se.loader.continous.write;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonProcessingException;

import com.cloudant.client.api.Database;
import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.loader.exception.StructureException;
import com.cloudant.se.writer.CloudantWriter;

public abstract class BaseFileLoader extends CloudantWriter {
	protected static final Logger	log	= Logger.getLogger(BaseFileLoader.class);
	private Configuration			config;
	private Path					fileName;
	private Path					path;
	private Path					dirCompleted;
	private Path					dirFailed;

	public BaseFileLoader(Configuration config, Database database, Path dirCompleted, Path dirFailed, Path name, Path path) {
		super(database);

		this.config = config;
		this.database = database;

		this.dirCompleted = dirCompleted;
		this.dirFailed = dirFailed;

		this.fileName = name;
		this.path = path;
	}

	@Override
	public WriteCode call() throws Exception {
		Map<String, Object> map = getContentsAsMap(path);

		String id = null;
		if ("filename".equalsIgnoreCase(config.getString("write.id.source", "filename"))) {
			id = FilenameUtils.removeExtension(fileName.toString());
			map.put("_id", id);
		}

		WriteCode wc = upsert(id, map);
		Path newPath = null;
		switch (wc) {
			case INSERT:
			case UPDATE:
				newPath = dirCompleted.resolve(fileName + "." + System.currentTimeMillis());
				break;
			default:
				newPath = dirFailed.resolve(fileName + "." + System.currentTimeMillis());
				break;
		}

		log.debug("[id=" + id + "] - move - \"" + path + "\" --> \"" + newPath + "\"");
		Files.move(path, newPath, REPLACE_EXISTING);

		return wc;
	}

	@Override
	protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
		//
		// In this base version, all we want is the latest revision number
		Map<String, Object> fromC = getFromCloudant((String) failed.get("_id"));
		failed.put("_rev", fromC.get("_rev"));

		return failed;
	}

	protected abstract Map<String, Object> getContentsAsMap(Path path) throws IOException;
}
