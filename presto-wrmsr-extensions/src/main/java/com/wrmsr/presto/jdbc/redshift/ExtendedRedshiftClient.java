package com.wrmsr.presto.jdbc.redshift;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClient;

import java.sql.SQLException;

/**
 * Created by wtimoney on 6/5/15.
 */
public class ExtendedRedshiftClient
    extends ExtendedPostgreSqlClient
{
    public ExtendedRedshiftClient(JdbcConnectorId connectorId, BaseJdbcConfig config) throws SQLException
    {
        super(connectorId, config);
    }
}
