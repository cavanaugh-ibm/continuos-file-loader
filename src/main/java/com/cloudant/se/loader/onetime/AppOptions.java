package com.cloudant.se.loader.onetime;

import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public class AppOptions {
	public static final Logger	log			= Logger.getLogger(App.class);

	@Parameter(names = { "-c", "-config" }, description = "The configuration file to load from", required = true)
	protected String			configFileName;

	@Parameter(names = { "-log", "-verbose" }, description = "Level of verbosity")
	protected Integer			verbose		= 0;

	@Parameter(names = { "-tracewrite" }, description = "Trace writing code", hidden = true)
	protected boolean			traceWrite	= false;

	@Parameter(names = { "-?", "--help" }, help = true, description = "Display this help")
	protected boolean			help;

	@Override
	public String toString() {
		return "AppOptions [configFileName=" + configFileName + ", verbose=" + verbose + ", help=" + help + "]";
	}
}
