package com.wrmsr.presto.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class ScriptingConfig
{
    private String engine;

    @NotNull
    public String getEngine()
    {
        return engine;
    }

    @Config("engine")
    public void setEngine(String engine)
    {
        this.engine = engine;
    }
}
