package com.wrmsr.presto.jdbc.sqlite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SqliteClient
    extends ExtendedJdbcClient
{
    public SqliteClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig)
    {
        super(connectorId, config, extendedConfig, "\"", null);
    }

    @Override
    public Connection getConnection(String url, Properties info) throws SQLException
    {
        return DriverManager.getConnection(url, info);
    }
}
