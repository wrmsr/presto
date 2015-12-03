package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

// TODO sxec / file / script
public class DoConfigNode
    extends ListConfigNode<DoConfigNode.Entry>
{
    @JsonCreator
    public static DoConfigNode valueOf(Object object)
    {

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

    }

    public static final class FileEntry extends Entry
    {
    }
}
