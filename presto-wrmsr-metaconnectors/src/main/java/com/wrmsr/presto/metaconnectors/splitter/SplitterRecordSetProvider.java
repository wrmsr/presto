package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class SplitterRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ConnectorRecordSetProvider target;

    public SplitterRecordSetProvider(ConnectorRecordSetProvider target)
    {
        this.target = checkNotNull(target);
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        return target.getRecordSet(((SplitterSplit) split).getTarget(), columns);
    }

    /*
    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        TpchSplit tpchSplit = checkType(split, TpchSplit.class, "split");

        String tableName = tpchSplit.getTableHandle().getTableName();

        TpchTable<?> tpchTable = TpchTable.getTable(tableName);

        return getRecordSet(tpchTable, columns, tpchSplit.getTableHandle().getScaleFactor(), tpchSplit.getPartNumber(), tpchSplit.getTotalParts());
    }
    */
}
