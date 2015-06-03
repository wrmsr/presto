package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Inject;

import java.sql.Driver;

public class ChunkedJdbcClient
    extends BaseJdbcClient
{
    @Inject
    public ChunkedJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote, Driver driver)
    {
        super(connectorId, config, identifierQuote, driver);
    }
}
