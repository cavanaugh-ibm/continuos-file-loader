package com.cloudant.se.loader.file;

import com.beust.jcommander.Parameter;
import com.cloudant.se.app.BaseAppOptions;

public class AppOptions extends BaseAppOptions {
    @Parameter(names = { "-watch" }, description = "Enables watching the configured source directory")
    public boolean watch = false;
}
