package com.wrmsr.presto.jdbc.sqlite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Inject;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;

public class SqliteClient
    extends ExtendedJdbcClient
{
    @Inject
    public SqliteClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig)
    {
        super(connectorId, config, extendedConfig, "\"", createDriver(extendedConfig, () -> null));
    }
}
