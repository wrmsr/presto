package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Configs;

import java.util.Map;

public final class ConnectorsConfigNode
    extends MapConfigNode<String, ConnectorsConfigNode.Entry>
{
    public static class Entry
    {
        @JsonCreator
        public static Entry valueOf(Object object)
        {
            return new Entry(Configs.flatten(object));
        }

        private final Map<String, String> entries;

        public Entry(Map<String, String> entries)
        {
            this.entries = ImmutableMap.copyOf(entries);
        }

        @JsonValue
        public Map<String, String> getEntries()
        {
            return entries;
        }

        @Override
        public String toString()
        {
            return "Entry{" +
                    "entries=" + entries +
                    '}';
        }
    }

    @JsonCreator
    public static ConnectorsConfigNode valueOf(Map<String, Entry> entries)
    {
        return new ConnectorsConfigNode(entries);
    }

    public ConnectorsConfigNode(Map<String, Entry> entries)
    {
        super(entries);
    }
}
