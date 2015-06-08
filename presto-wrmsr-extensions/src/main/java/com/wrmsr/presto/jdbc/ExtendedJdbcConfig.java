package com.wrmsr.presto.jdbc;

import io.airlift.configuration.Config;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

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

    private final List<String> initScripts = newArrayList();

    public List<String> getInitScripts()
    {
        return initScripts;
    }

    private boolean isRemotelyAccessible = true;

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
