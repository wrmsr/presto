package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.*;

import java.util.List;

public class StaticPartitioner implements Partitioner
{
    private final ConnectorMetadata metadata;

    public StaticPartitioner(ConnectorMetadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public List<ConnectorPartition> getPartitionsConnector(SchemaTableName table, TupleDomain<ColumnHandle> tupleDomain)
    {
        return null;
    }
}
