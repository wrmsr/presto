package com.wrmsr.presto.jdbc.sqlite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.*;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class SqliteClientModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        try {
            Class.forName("org.sqlite.JDBC");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        binder.bind(JdbcClient.class).to(SqliteClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ExtendedJdbcConfig.class);
    }
}
