package com.wrmsr.presto.jdbc;

import io.airlift.configuration.Config;

public class ExtendedJdbcConfig
{
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

    private String driverClass;

    public String getDriverClass()
    {
        return driverClass;
    }

    @Config("driver-class")
    public void setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
    }

    private String init;

    public String getInit()
    {
        return init;
    }

    @Config("init")
    public void setInit(String init)
    {
        this.init = init;
    }

    private boolean isRemotelyAccessible;

    public boolean getIsRemotelyAccessible()
    {
        return isRemotelyAccessible;
    }

    @Config("is-remotely-accessible")
    public void setIsRemotelyAccessible(boolean isRemotelyAccessible)
    {
        this.isRemotelyAccessible = isRemotelyAccessible;
    }
}
