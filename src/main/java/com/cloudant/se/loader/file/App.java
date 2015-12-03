package com.cloudant.se.loader.file;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static com.cloudant.se.util.UConfig.ensureReadWriteDirectory;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import com.cloudant.se.app.BaseApp;
import com.cloudant.se.loader.file.read.DirWatcher;
import com.cloudant.se.loader.file.read.DirWatcherCallback;
import com.cloudant.se.loader.file.write.JsonFileLoader;

/**
 * @author Cavanaugh
 */
public class App extends BaseApp implements DirWatcherCallback {
    private DirWatcher watcher       = null;
    protected Path     dirCompleted  = null;
    protected Path     dirFailed     = null;
    protected Path     dirProcessing = null;
    protected Path     dirStaging    = null;
    protected boolean  watching      = false;

    public App(String name) {
        super(name, new AppOptions());
    }

    @Override
    public void fileCreated(Path path) throws IOException {
        //
        // Load the file that was just created
        Path tempPath = moveToProcessing(dirStaging.resolve(path));
        log.info("Queueing processing for " + tempPath);
        submitCallable(new JsonFileLoader(getDatabase(), dirCompleted, dirFailed, tempPath, useFilenameAsSource(), mergeWithExisting(), getVersionField(), getIdSourceFields()));
    }

    @Override
    public void validateConfig() {
        dirStaging = ensureReadWriteDirectory(getConfiguration(), "dir.staging", "staging").toPath();
        dirProcessing = ensureReadWriteDirectory(getConfiguration(), "dir.processing", "processing").toPath();
        dirCompleted = ensureReadWriteDirectory(getConfiguration(), "dir.completed", "completed").toPath();
        dirFailed = ensureReadWriteDirectory(getConfiguration(), "dir.failed", "failed").toPath();

        watching = getConfigBool("watch", false);
        watcher = new DirWatcher(dirStaging, this);

        if (mergeWithExisting()) {
            log.warn("MERGE asked for - array elements may not merge correctly");
        }
    }

    protected void mergeOptions() {
        super.mergeOptions();

        if (((AppOptions) getOptions()).watch || getConfigBool("watch", false)) {
            setConfigValue("watch", true);
        }
    }

    private String getVersionField() {
        String versionField = getConfigStr("write.version.field", null);

        if (StringUtils.isNotBlank(versionField)) {
            throw new IllegalArgumentException("VERSION checking is not implemented yet");
        }
        return versionField;
    }

    private String[] getIdSourceFields() {
        String[] fields = getConfigStrArray("write.id.fields");
        Assert.notEmpty(fields, "Configuration must provide a valid list of fields to pull id from [write.id.fields]");
        return fields;
    }

    private boolean mergeWithExisting() {
        return getConfigBool("write.merge", false);
    }

    private boolean useFilenameAsSource() {
        return getConfigBool("write.id.usefilename", false);
    }

    @Override
    protected void doWork() {
        if (watching) {
            watcher.startWatching();
        } else {
            //
            // Process all the files that are in the directory
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirStaging)) {
                for (Path path : stream) {
                    Path tempPath = moveToProcessing(path);
                    log.info("Queueing processing for " + tempPath);
                    submitCallable(new JsonFileLoader(getDatabase(), dirCompleted, dirFailed, tempPath, useFilenameAsSource(), mergeWithExisting(), getVersionField(), getIdSourceFields()));
                }
            } catch (IOException | DirectoryIteratorException e) {
                log.fatal("Unable to read from the staging directory", e);
            }
        }
    }

    public static void main(String[] args)
    {
        new App("Cloudandt \"JSON Directory Loader\"").run(args);
    }

    /*
     * Move a file into the processing directory with a unique name
     */
    protected Path moveToProcessing(Path path) throws IOException {
        Path newName = Paths.get(path.getFileName() + JsonFileLoader.KEYWORD_PROCESSING + UUID.randomUUID());
        Path newPath = dirProcessing.resolve(newName);

        log.debug(format("Moving to processing %s --> %s", path, newPath));
        return Files.move(path, newPath, ATOMIC_MOVE);
    }

    @Override
    public void fileModified(Path path) throws IOException {
    }

    @Override
    public void fileDeleted(Path path) throws IOException {
    }
}
