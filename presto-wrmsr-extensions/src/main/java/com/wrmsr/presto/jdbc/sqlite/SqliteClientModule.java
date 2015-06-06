package com.wrmsr.presto.jdbc.sqlite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
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
