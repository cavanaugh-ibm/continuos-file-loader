package com.cloudant.se.loader.file.write;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.log4j.Logger;

import com.cloudant.client.api.Database;
import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.writer.CloudantWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public abstract class BaseFileLoader extends CloudantWriter {
	protected static final Logger	log			= Logger.getLogger(BaseFileLoader.class);
	protected Configuration			config;
	protected Path					dirCompleted;
	protected Path					dirFailed;
	protected Path					fileName;
	protected Path					path;
	protected Joiner				keyJoiner	= Joiner.on("_").skipNulls();

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
		String id = null;
		try {
			Map<String, Object> map = getContentsAsMap();
			id = getId(map);

			map.put("_id", id);
			WriteCode wc = upsert(id, map);

			moveFile(id, wc);

			return wc;
		} catch (Exception e) {
			log.warn("[id=" + id + "] - exception", e);
			moveFile(id, WriteCode.EXCEPTION);
			return WriteCode.EXCEPTION;
		}
	}

	protected abstract Map<String, Object> getContentsAsMap() throws IOException;

	protected String getId(Map<String, Object> map) {
		String id = null;
		String idSource = config.getString("write.id.source", "filename");

		if ("filename".equalsIgnoreCase(idSource)) {
			id = FilenameUtils.removeExtension(fileName.toString());
		} else if ("fields".equalsIgnoreCase(idSource)) {
			String[] idFields = config.getStringArray("write.id.fields");

			List<Object> values = Lists.newArrayList();
			JXPathContext context = JXPathContext.newContext(map);

			for (String field : idFields) {
				values.add(context.getValue(field));
			}

			id = keyJoiner.join(values);
		}

		return id;
	}

	@Override
	protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
		//
		// In this base version, all we want is the latest revision number
		Map<String, Object> fromC = get((String) failed.get("_id"));
		failed.put("_rev", fromC.get("_rev"));

		return failed;
	}

	protected void moveFile(String id, WriteCode wc) {
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

		try {
			log.debug("[id=" + id + "] - move - \"" + path + "\" --> \"" + newPath + "\"");
			Files.move(path, newPath, REPLACE_EXISTING);
		} catch (IOException e) {
			log.warn("[id=" + id + "] - move - FAILED - \"" + path + "\" --> \"" + newPath + "\"", e);
		}
	}
}
