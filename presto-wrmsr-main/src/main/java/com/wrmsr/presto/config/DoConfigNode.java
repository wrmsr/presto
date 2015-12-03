package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;

import java.util.List;

// TODO sxec / file / script
public class DoConfigNode
    extends ListConfigNode<DoConfigNode.Entry>
{
    @JsonCreator
    public static DoConfigNode valueOf(Object object)
    {
        List<Entry> entries;
        if (object instanceof String) {
            entries = ImmutableList.of(new StatementEntry((String) object));
        }
        else {
            throw new IllegalArgumentException();
        }
        return new DoConfigNode(entries);
    }

    public DoConfigNode(List<Entry> items)
    {
        super(items);
    }

    @JsonValue
    public List<Entry> getItems()
    {
        return items;
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = StatementEntry.class, name = "statement"),
            @JsonSubTypes.Type(value = FileEntry.class, name = "file"),
    })
    public static abstract class Entry
    {
    }

    public static final class StatementEntry extends Entry
    {
        private final String statement;

        @JsonCreator
        public static StatementEntry valueOf(String statement)
        {
            return new StatementEntry(statement);
        }

        public StatementEntry(String statement)
        {
            this.statement = statement;
        }

        @JsonValue
        public String getStatement()
        {
            return statement;
        }
    }

    public static final class FileEntry extends Entry
    {
        private final String path;

        @JsonCreator
        public static FileEntry valueOf(String path)
        {
            return new FileEntry(path);
        }

        public FileEntry(String path)
        {
            this.path = path;
        }

        @JsonValue
        public String getPath()
        {
            return path;
        }
    }
}
