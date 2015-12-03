package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

// TODO sxec / file / script
public class DoConfigNode
        extends ListConfigNode<DoConfigNode.Entry>
{
    @JsonCreator
    public static DoConfigNode valueOf(Object object)
    {
        ObjectMapper mapper = OBJECT_MAPPER.get();
        List<Entry> entries;
        if (object instanceof String) {
            entries = ImmutableList.of(new StatementEntry((String) object));
        }
        else if (object instanceof Entry) {
            entries = ImmutableList.of((Entry) object);
        }
        else if (object instanceof List) {
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            for (Object o : (List) object) {
                if (o instanceof String) {
                    builder.add(new StatementEntry((String) o));
                }
                else if (o instanceof Map) {
                    try {
                        builder.add(mapper.readValue(mapper.writeValueAsString(o), Entry.class));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                else {
                    throw new IllegalArgumentException(Objects.toString(o));
                }
            }
            entries = builder.build();
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
            @JsonSubTypes.Type(value = StatementEntry.class, name = StatementEntry.NAME),
            @JsonSubTypes.Type(value = FileEntry.class, name = FileEntry.NAME),
    })
    public static abstract class Entry
    {
    }

    public static final class StatementEntry
            extends Entry
    {
        public static final String NAME = "statement";

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

    public static final class FileEntry
            extends Entry // TODO os.path.expandvars
    {
        public static final String NAME = "file";

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
