package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcRecordSinkProvider;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;

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
