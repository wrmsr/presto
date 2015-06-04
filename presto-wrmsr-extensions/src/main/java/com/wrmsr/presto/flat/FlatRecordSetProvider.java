package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.inject.Inject;

import java.util.List;

public class FlatRecordSetProvider
    implements ConnectorRecordSetProvider
{
    @Inject
    public FlatRecordSetProvider()
    {
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        return new FlatRecordSet();
    }
}
