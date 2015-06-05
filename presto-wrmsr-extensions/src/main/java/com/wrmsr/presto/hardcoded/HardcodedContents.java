package com.wrmsr.presto.hardcoded;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HardcodedContents
{
    private final Map<String, String> views;

    public HardcodedContents(Map<String, String> views)
    {
        this.views = checkNotNull(views);
    }

    public Map<String, String> getViews()
    {
        return views;
    }
}
