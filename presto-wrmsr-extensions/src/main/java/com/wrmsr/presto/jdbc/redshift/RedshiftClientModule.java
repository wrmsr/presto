package com.wrmsr.presto.jdbc.redshift;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClient;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RedshiftClientModule
    implements Module
{
    public static final String DEFAULT_DRIVER_URL = "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.1.0001.jar";

    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).to(ExtendedPostgreSqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ExtendedJdbcConfig.class);
    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("driver-url", DEFAULT_DRIVER_URL)
                .build();
    }
}
