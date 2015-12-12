package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.mergeable.MapMergeableConfigNode;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Map;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public final class ConnectorsConfig
    extends MapMergeableConfigNode<ConnectorsConfig, String, ConnectorsConfig.Entry> implements MainConfigNode<ConnectorsConfig>
{
    public static final class Entry
    {
        @JsonCreator
        public static Entry valueOf(Object object)
        {
            return new Entry(Configs.flatten(object));
        }

        protected final Map<String, String> entries;

        public Entry(Map<String, String> entries)
        {
            this.entries = ImmutableMap.copyOf(entries);
        }

        @JsonValue
        public Map<String, String> getEntries()
        {
            return entries;
        }
    }

    @JsonCreator
    public ConnectorsConfig(Map<String, Entry> entries)
    {
        super(entries);
    }

    @JsonValue
    public Map<String, Map<String, String>> flatten()
    {
        return getEntries().entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), e.getValue().getEntries())).collect(toImmutableMap());
    }
}
