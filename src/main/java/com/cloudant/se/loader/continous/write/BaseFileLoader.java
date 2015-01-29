package com.cloudant.se.loader.continous.write;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonProcessingException;

import com.cloudant.client.api.Database;
import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.loader.exception.StructureException;
import com.cloudant.se.writer.CloudantWriter;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;

public abstract class BaseFileLoader extends CloudantWriter {
	protected static final Logger	log			= Logger.getLogger(BaseFileLoader.class);
	protected Joiner				keyJoiner	= Joiner.on("_").skipNulls();
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
		try {
			Map<String, Object> map = getContentsAsMap(path);

			String id = null;
			String idSource = config.getString("write.id.source", "filename");
			if ("filename".equalsIgnoreCase(idSource)) {
				id = FilenameUtils.removeExtension(fileName.toString());
			} else if ("fields".equalsIgnoreCase(idSource)) {
				String[] idFields = config.getStringArray("write.id.fields");

				List<Object> values = Lists.newArrayList();

				//
				// Hack for now - inefficient
				for (String field : idFields) {
					values.add(JsonPath.read(path.toFile(), field));
				}

				id = keyJoiner.join(values);
			}

			map.put("_id", id);
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
		} catch (Exception e) {
			e.printStackTrace();
			Files.move(path, dirFailed.resolve(fileName + "." + System.currentTimeMillis()), REPLACE_EXISTING);
			return WriteCode.EXCEPTION;
		}
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
