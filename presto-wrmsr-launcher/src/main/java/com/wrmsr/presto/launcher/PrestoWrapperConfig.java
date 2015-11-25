package com.wrmsr.presto.launcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties
public class PrestoWrapperConfig
{
    private final Map<String, String> jvm;
    private final Map<String, String> system;
    private final Map<String, String> log;

    @JsonCreator

    public PrestoWrapperConfig(
            @JsonProperty("jvm") Map<String, String> jvm,
            @JsonProperty("system") Map<String, String> system,
            @JsonProperty("log") Map<String, String> log)
    {
        this.jvm = jvm;
        this.system = system;
        this.log = log;
    }

    @JsonProperty("jvm")
    public Map<String, String> getJvm()
    {
        return jvm;
    }

    @JsonProperty("system")
    public Map<String, String> getSystem()
    {
        return system;
    }

    @JsonProperty("log")
    public Map<String, String> getLog()
    {
        return log;
    }
}
