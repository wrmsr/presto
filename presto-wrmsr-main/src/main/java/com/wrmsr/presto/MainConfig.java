package com.wrmsr.presto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.scripting.ScriptingConfig;

import java.util.List;
import java.util.Map;

public class MainConfig
{
    private final Map<String, String> jvm;
    private final Map<String, String> system;
    private final Map<String, String> log;

    private final List<String> plugins;
    private final Map<String, Object> connectors;
    private final Map<String, ScriptingConfig> scripting;

    // FIXME gp plugin section
    // public final Map<String, Object> clusters = ImmutableMap.of();
    // public final Object aws = null;

    @JsonCreator
    public MainConfig(
            @JsonProperty("jvm") Map<String, String> jvm,
            @JsonProperty("system") Map<String, String> system,
            @JsonProperty("log") Map<String, String> log,
            @JsonProperty("plugins") List<String> plugins,
            @JsonProperty("connectors") Map<String, Object> connectors,
            @JsonProperty("scripting") Map<String, ScriptingConfig> scripting)
    {
        this.jvm = jvm;
        this.system = system;
        this.log = log;
        this.plugins = plugins;
        this.connectors = connectors;
        this.scripting = scripting;
    }

    @JsonProperty
    public Map<String, String> getJvm()
    {
        return jvm;
    }

    @JsonProperty
    public Map<String, String> getSystem()
    {
        return system;
    }

    @JsonProperty
    public Map<String, String> getLog()
    {
        return log;
    }

    @JsonProperty
    public List<String> getPlugins()
    {
        return plugins;
    }

    @JsonProperty
    public Map<String, Object> getConnectors()
    {
        return connectors;
    }

    @JsonProperty
    public Map<String, ScriptingConfig> getScripting()
    {
        return scripting;
    }
}
