package com.wrmsr.presto.jdbc.temp;

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.jdbc.h2.H2ClientModule;

import java.util.Map;

import static java.lang.String.format;

public class TempClientModule
    extends H2ClientModule
{
    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime()))
                .put("is-remoteely-accessible", "false")
                .put("init", "CREATE SCHEMA temp")
                .build();
    }
}
