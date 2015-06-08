package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.google.inject.Inject;

public class ExtendedJdbcHandleResolver
    extends JdbcHandleResolver
{
    @Inject
    public ExtendedJdbcHandleResolver(JdbcConnectorId clientId)
    {
        super(clientId);
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return ExtendedJdbcSplit.class;
    }

}
