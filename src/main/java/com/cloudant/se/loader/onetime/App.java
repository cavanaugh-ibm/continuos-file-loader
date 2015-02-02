package com.cloudant.se.loader.onetime;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.ConnectOptions;
import com.cloudant.se.concurrent.StatusingThreadPoolExecutor;
import com.cloudant.se.loader.file.write.BaseFileLoader;
import com.cloudant.se.loader.file.write.JsonFileLoader;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Cavanaugh
 */
public class App {
	private static final Logger	log				= Logger.getLogger(App.class);
	protected AppOptions		options			= null;
	protected Configuration		config			= null;
	protected ExecutorService	writerExecutor	= null;

	protected Path				dirStaging		= null;
	protected Path				dirCompleted	= null;
	protected Path				dirFailed		= null;
	private CloudantClient		client;
	private Database			database;

	public App() {
	}

	public int config(String[] args) {
		options = new AppOptions();
		JCommander jCommander = new JCommander();
		jCommander.setProgramName("Cloudandt \"One-Time File Loader\"");
		jCommander.addObject(options);

		//
		// Try to parse the options we were given
		try {
			jCommander.parse(args);
		} catch (ParameterException e) {
			showUsage(jCommander);
			return 1;
		}

		//
		// Show the help if they asked for it
		if (options.help) {
			showUsage(jCommander);
			return 2;
		}

		//
		// Enable debugging if asked
		if (options.verbose > 0) {
			// Logger.getRootLogger().setLevel(Level.DEBUG);
			if (options.verbose >= 1) {
				Logger.getLogger("com.cloudant").setLevel(Level.DEBUG);
			}
			if (options.verbose >= 2) {
				Logger.getLogger("org.lightcouch").setLevel(Level.DEBUG);
			}
			if (options.verbose >= 3) {
				Logger.getLogger("org.apache.http").setLevel(Level.DEBUG);
			}
		}

		//
		// Enable tracing if asked
		if (options.traceWrite) {
			Logger.getLogger(BaseFileLoader.class.getPackage().getName()).setLevel(Level.TRACE);
		}

		//
		// Read the config they gave us
		try {
			//
			// Read the configuration from our file and let it validate itself
			config = new PropertiesConfiguration(options.configFileName);

			dirStaging = getPath(config, "dir.staging", "staging");
			dirCompleted = getPath(config, "dir.completed", "completed");
			dirFailed = getPath(config, "dir.failed", "failed");
		} catch (IllegalArgumentException e) {
			System.err.println("Configuration error detected - " + e.getMessage());
			config = null;
			return -2;
		} catch (Exception e) {
			System.err.println("Unexpected exception - see log for details - " + e.getMessage());
			log.error(e.getMessage(), e);
			return -1;
		}

		int threads = config.getInt("write.threads");
		BlockingQueue<Runnable> blockingQueue = new LinkedBlockingDeque<>(threads * 3);
		ThreadFactory writeThreadFactory = new ThreadFactoryBuilder().setNameFormat("ldr-w-%d").build();
		writerExecutor = new StatusingThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, blockingQueue, writeThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

		return 0;
	}

	private Path getPath(Configuration config, String key, String defaultName) {
		File file = new File(config.getString(key, defaultName));

		if (file.exists()) {
			if (file.isDirectory()) {
				if (file.canRead() && file.canWrite()) {
					return file.toPath();
				} else {
					throw new IllegalArgumentException(key + " must point at a directory that we can read/write to");
				}
			} else {
				throw new IllegalArgumentException(key + " must point at a directory not a file");
			}
		} else {
			log.info("Creating directory for \"" + key + "\" --> " + file.getAbsolutePath());
			if (file.mkdirs()) {
				return file.toPath();
			} else {
				throw new RuntimeException("Unable to create directory for \"" + key + "\"");
			}
		}
	}

	private void showUsage(JCommander jCommander) {
		jCommander.usage();
	}

	private int start() {
		log.info("Configuration complete, starting up");
		try {
			ConnectOptions options = new ConnectOptions();
			options.setMaxConnections(config.getInt("write.threads"));

			log.info(" --- Connecting to Cloudant --- ");
			client = new CloudantClient(config.getString("cloudant.account"), config.getString("cloudant.user"), config.getString("cloudant.pass"), options);
			database = client.database(config.getString("cloudant.database"), false);
			log.info(" --- Connected to Cloudant --- ");
		} catch (Exception e) {
			log.fatal("Unable to connect to the database", e);
			return -4;
		}

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirStaging)) {
			for (Path file : stream) {
				log.info("Queueing processing for " + file.getFileName());
				writerExecutor.submit(new JsonFileLoader(config, database, dirCompleted, dirFailed, file.getFileName(), file));
			}
		} catch (IOException | DirectoryIteratorException e) {
			log.fatal("Unable to read from the staging directory", e);
		}

		log.info("Waiting for writers to complete");
		writerExecutor.shutdown();

		try {
			writerExecutor.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
		}
		log.info("All writers have completed");

		log.info("App complete, shutting down");
		return 0;
	}

	public static void main(String[] args)
	{
		App app = new App();
		int configReturnCode = app.config(args);
		switch (configReturnCode) {
			case 0:
				// config worked, user accepted design
				System.exit(app.start());
				break;
			default:
				// config did NOT work, error out
				System.exit(configReturnCode);
				break;
		}
	}

	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}
}
