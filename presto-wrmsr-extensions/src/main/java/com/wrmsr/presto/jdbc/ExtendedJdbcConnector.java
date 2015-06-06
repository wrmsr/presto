package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.*;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExtendedJdbcConnector
    extends JdbcConnector
{
    private final JdbcClient jdbcClient;

    @Inject
    public ExtendedJdbcConnector(LifeCycleManager lifeCycleManager, JdbcMetadata jdbcMetadata, JdbcSplitManager jdbcSplitManager, JdbcRecordSetProvider jdbcRecordSetProvider, JdbcHandleResolver jdbcHandleResolver, JdbcRecordSinkProvider jdbcRecordSinkProvider, JdbcClient jdbcClient)
    {
        super(lifeCycleManager, jdbcMetadata, jdbcSplitManager, jdbcRecordSetProvider, jdbcHandleResolver, jdbcRecordSinkProvider);
        this.jdbcClient = checkNotNull(jdbcClient);
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }
}
