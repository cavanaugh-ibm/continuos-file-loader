package com.cloudant.se.loader.file.read;

import java.io.IOException;
import java.nio.file.Path;

public interface DirWatcherCallback {
    public void fileCreated(Path path) throws IOException;

    public void fileModified(Path path) throws IOException;

    public void fileDeleted(Path path) throws IOException;
}
