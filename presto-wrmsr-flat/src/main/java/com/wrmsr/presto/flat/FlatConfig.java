package com.wrmsr.presto.flat;

import io.airlift.configuration.Config;

public class FlatConfig
{
    private String filePath;

    public String getFilePath()
    {
        return filePath;
    }

    @Config("file-path")
    public void setFilePath(String filePath)
    {
        this.filePath = filePath;
    }
}
