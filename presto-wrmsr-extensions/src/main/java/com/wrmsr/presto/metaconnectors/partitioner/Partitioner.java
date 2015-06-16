package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;

import java.util.List;

@FunctionalInterface
public interface Partitioner
{
    List<ConnectorPartition> getPartitionsConnector(SchemaTableName table, TupleDomain<ColumnHandle> tupleDomain);
}
