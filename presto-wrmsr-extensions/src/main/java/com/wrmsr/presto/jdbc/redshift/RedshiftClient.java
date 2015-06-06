package com.wrmsr.presto.jdbc.redshift;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Inject;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClient;

import java.sql.SQLException;

public class RedshiftClient
    extends ExtendedPostgreSqlClient
{
    @Inject
    public RedshiftClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig) throws SQLException
    {
        super(connectorId, config, extendedConfig);
    }
}
