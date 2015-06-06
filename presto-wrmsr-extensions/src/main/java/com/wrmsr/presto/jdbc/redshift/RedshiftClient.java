package com.wrmsr.presto.jdbc.redshift;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClient;

import java.sql.SQLException;

public class RedshiftClient
    extends ExtendedPostgreSqlClient
{
    public RedshiftClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException
    {
        super(connectorId, config);
    }
}
