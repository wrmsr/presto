package com.wrmsr.presto.jdbc;

import io.airlift.configuration.Config;

public class ExtendedJdbcConfig
{
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
}
