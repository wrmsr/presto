package com.wrmsr.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

public class MainConfig
{
    public final Map<String, String> jvm = ImmutableMap.of();
    public final Map<String, String> system = ImmutableMap.of();
    public final Map<String, String> log = ImmutableMap.of();

    public final List<String> plugins = ImmutableList.of();
    public final Map<String, Object> connectors = ImmutableMap.of();

    // FIXME gp plugin section
    public final Map<String, Object> clusters = ImmutableMap.of();
    public final Object aws = null;
}
