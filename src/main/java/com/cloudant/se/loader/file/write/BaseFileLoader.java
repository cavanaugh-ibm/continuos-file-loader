package com.cloudant.se.loader.file.write;

import static com.cloudant.se.db.writer.CloudantWriteResult.errorResult;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import com.cloudant.client.api.Database;
import com.cloudant.se.Constants.WriteCode;
import com.cloudant.se.db.exception.StructureException;
import com.cloudant.se.db.writer.CloudantWriteResult;
import com.cloudant.se.db.writer.CloudantWriter;
import com.cloudant.se.util.UMap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public abstract class BaseFileLoader extends CloudantWriter {
    public static final String    KEYWORD_PROCESSING = "_processing_";
    protected static final Logger log                = Logger.getLogger(BaseFileLoader.class);
    protected static final Joiner keyJoiner          = Joiner.on("_").skipNulls();
    protected Path                dirCompleted       = null;
    protected Path                dirFailed          = null;
    protected Path                path               = null;
    protected boolean             idFromFilename     = true;
    protected String[]            idSourceFields     = null;
    protected boolean             mergeWithExisting  = false;
    protected String              versionField       = null;

    public BaseFileLoader(Database database, Path dirCompleted, Path dirFailed, Path path, boolean idFromFilename, boolean mergeWithExisting, String versionField, String... idSourceFields) {
        super(database);

        this.database = database;
        this.dirCompleted = dirCompleted;
        this.dirFailed = dirFailed;
        this.path = path;
        this.idFromFilename = idFromFilename;
        this.mergeWithExisting = mergeWithExisting;
        this.versionField = versionField;

        if (!idFromFilename) {
            Assert.notEmpty(idSourceFields, "Must provide a list of fields to create the _id from");
            this.idSourceFields = idSourceFields;
        }
    }

    @Override
    public CloudantWriteResult call() throws Exception {
        String id = null;
        try {
            Map<String, Object> map = getContentsAsMap();
            id = getId(map);

            map.put("_id", id);
            CloudantWriteResult result = upsert(id, map);

            moveFile(id, result);

            return result;
        } catch (Exception e) {
            log.warn("[id=" + id + "] - exception", e);
            CloudantWriteResult result = errorResult(WriteCode.EXCEPTION, e);
            moveFile(id, result);
            return result;
        }
    }

    protected abstract Map<String, Object> getContentsAsMap() throws IOException;

    protected String getId(Map<String, Object> map) {
        String id = null;

        if (idFromFilename) {
            id = FilenameUtils.removeExtension(path.getFileName().toString());
        } else {
            List<Object> values = Lists.newArrayList();
            JXPathContext context = JXPathContext.newContext(map);

            for (String field : idSourceFields) {
                values.add(context.getValue(field));
            }

            id = keyJoiner.join(values);
        }

        return id;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Object> handleConflict(Map<String, Object> failed) throws StructureException, JsonProcessingException, IOException {
        Map<String, Object> fromC = get((String) failed.get("_id"));
        if (mergeWithExisting) {
            //
            // If we are merging, we want to add in all the new information (except the revision) (DEEP MERGE)
            failed.remove("_rev");
            return UMap.deepMerge(fromC, failed);
        } else {
            //
            // If we are not merging, we are replacing so all we want from the previous version is the revision
            failed.put("_rev", fromC.get("_rev"));
            return failed;
        }
    }

    protected void moveFile(String id, CloudantWriteResult result) {
        Path newPath = null;
        String fileNameString = path.getFileName().toString();
        String baseName = fileNameString.substring(0, fileNameString.indexOf(KEYWORD_PROCESSING));

        //
        // TODO add in information about what we did - rev info for instance
        //
        switch (result.getWriteCode()) {
            case INSERT:
            case UPDATE:
                newPath = dirCompleted.resolve(format("%s__ID-%s__REV-%s__UUID-%s", baseName, result.getId(), result.getRev(), UUID.randomUUID()));
                break;
            default:
                newPath = dirFailed.resolve(format("%s__CODE-%s__UUID-%s", baseName, result.getWriteCode(), UUID.randomUUID()));
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
