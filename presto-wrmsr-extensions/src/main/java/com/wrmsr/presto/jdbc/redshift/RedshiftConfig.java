package com.wrmsr.presto.jdbc.redshift;

import io.airlift.configuration.Config;

public class RedshiftConfig
{
    public static final String DEFAULT_DRIVER_URL = "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.1.0001.jar";

    private String driverUrl;

    public String getDriverUrl()
    {
        return driverUrl;
    }

    @Config("driver-url")
    public void setDriverUrl(String driverUrl)
    {
        this.driverUrl = driverUrl;
    }
}
