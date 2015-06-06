package com.wrmsr.presto.jdbc.h2;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.inject.Inject;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;
import org.h2.Driver;

public class H2Client extends ExtendedJdbcClient
{
    @Inject
    public H2Client(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig)
    {
        super(connectorId, config, extendedConfig, "\"", new Driver());
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }
}
