package com.cloudant.se.loader.file.read;

import static java.lang.String.format;
import static name.pachler.nio.file.StandardWatchEventKind.ENTRY_CREATE;
import static name.pachler.nio.file.StandardWatchEventKind.OVERFLOW;

import java.io.IOException;
import java.util.List;

import name.pachler.nio.file.FileSystems;
import name.pachler.nio.file.Path;
import name.pachler.nio.file.Paths;
import name.pachler.nio.file.WatchEvent;
import name.pachler.nio.file.WatchEvent.Kind;
import name.pachler.nio.file.WatchKey;
import name.pachler.nio.file.WatchService;

import org.apache.log4j.Logger;

public class DirWatcher {
    private static final Logger log        = Logger.getLogger(DirWatcher.class);
    private DirWatcherCallback  callback   = null;
    private Path                directory  = null;

    private WatchService        watcher    = null;

    private WatchKey            watcherKey = null;

    public DirWatcher(java.nio.file.Path directory, DirWatcherCallback callback) {
        this.directory = convert(directory);
        this.callback = callback;
    }

    public DirWatcher(Path directory, DirWatcherCallback callback) {
        this.directory = directory;
        this.callback = callback;
    }

    public void startWatching() {
        log.info("DirWatcher starting up - " + directory);

        //
        // Register a shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (watcherKey != null) {
                    watcherKey.cancel();
                }
            }
        });

        try {
            //
            // Setup the actual watcher service
            watcher = FileSystems.getDefault().newWatchService();

            //
            // Start watching the staging directory
            watcherKey = directory.register(watcher, ENTRY_CREATE);
        } catch (Exception e) {
            log.fatal("Unexpected exception", e);
            return;
        }

        //
        // Forever loop waiting for files
        for (;;) {
            // wait for key to be signaled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                break;
            }

            // get list of events from key
            List<WatchEvent<?>> list = key.pollEvents();

            // VERY IMPORTANT! call reset() AFTER pollEvents() to allow the
            // key to be reported again by the watch service
            key.reset();

            for (WatchEvent<?> event : list) {
                Kind<?> kind = event.kind();

                if (kind == OVERFLOW) {
                    continue;
                }

                //
                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path path = ev.context();

                //
                // print out the raw event
                log.debug(format("%s: %s\n", event.kind().name(), path));

                if (kind == ENTRY_CREATE) {
                    // Let the callback do its thing
                    try {
                        callback.fileCreated(convert(path));
                    } catch (IOException e) {
                        log.error(format("Error handling %s", path), e);
                    }
                }
            }
        }

        log.info("DirWatcher shutting down - " + directory);
    }

    private Path convert(java.nio.file.Path path) {
        return Paths.get(path.toString());
    }

    private java.nio.file.Path convert(Path path) {
        return java.nio.file.Paths.get(path.toString());
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }
}
