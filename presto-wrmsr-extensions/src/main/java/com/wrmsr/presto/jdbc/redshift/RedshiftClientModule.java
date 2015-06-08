package com.wrmsr.presto.jdbc.redshift;

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClientModule;

import java.util.Map;

public class RedshiftClientModule
    extends ExtendedPostgreSqlClientModule
{
    public static final String DEFAULT_DRIVER_URL = "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.1.0001.jar";
    public static final String DEFAULT_DRIVER_CLASS = "com.amazon.redshift.jdbc4.Driver";

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("driver-url", DEFAULT_DRIVER_URL)
                .put("driver-class", DEFAULT_DRIVER_CLASS)
                .build();
    }
}
