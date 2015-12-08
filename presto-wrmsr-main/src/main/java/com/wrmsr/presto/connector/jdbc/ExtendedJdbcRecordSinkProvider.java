package com.wrmsr.presto.connector.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;

import javax.inject.Inject;

public class ExtendedJdbcRecordSinkProvider
    extends JdbcRecordSinkProvider
{
    @Inject
    public ExtendedJdbcRecordSinkProvider(JdbcClient jdbcClient)
    {
        super(jdbcClient);
    }

//    @Override
//    public RecordSink getRecordSink(ConnectorSession session, ConnectorInsertTableHandle tableHandle)
//    {
//        return getRecordSink(session, ((ExtendedJdbcInsertTableHandle) tableHandle).getOutputTableHandle());
//    }
}
