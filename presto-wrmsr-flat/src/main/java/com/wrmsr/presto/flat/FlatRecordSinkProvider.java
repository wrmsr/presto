package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.RecordSink;
import com.google.inject.Inject;

public class FlatRecordSinkProvider
    implements ConnectorRecordSinkProvider
{
    @Inject
    public FlatRecordSinkProvider()
    {
    }

    @Override
    public RecordSink getRecordSink(ConnectorOutputTableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordSink getRecordSink(ConnectorInsertTableHandle tableHandle)
    {
        return new FlatRecordSink();
    }
}
