package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.wrmsr.presto.util.config.merging.MapMergingConfigNode;

import java.util.Map;

public class ScriptingConfig
        extends MapMergingConfigNode<ScriptingConfig, String, ScriptingConfig.Entry>
        implements MainConfigNode<ScriptingConfig>
{
    public static final class Entry
    {
        private final String engine;

        @JsonCreator
        public Entry(
                @JsonProperty("engine") String engine)
        {
            this.engine = engine;
        }

        @JsonProperty
        public String getEngine()
        {
            return engine;
        }
    }

    @JsonCreator
    public ScriptingConfig(Map<String, Entry> entries)
    {
        super(entries);
    }

    @JsonValue
    @Override
    public Map<String, Entry> getEntries()
    {
        return super.getEntries();
    }
}
