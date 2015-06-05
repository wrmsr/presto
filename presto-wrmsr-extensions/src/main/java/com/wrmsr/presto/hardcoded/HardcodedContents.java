package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.SchemaTableName;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HardcodedContents
{
    private final Map<SchemaTableName, String> views;

    public HardcodedContents(Map<SchemaTableName, String> views)
    {
        this.views = checkNotNull(views);
    }

    public Map<SchemaTableName, String> getViews()
    {
        return views;
    }
}
