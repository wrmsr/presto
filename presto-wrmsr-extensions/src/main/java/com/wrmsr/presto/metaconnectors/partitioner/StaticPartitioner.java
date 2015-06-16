package com.wrmsr.presto.metaconnectors.partitioner;

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
    public List<Partition> getPartitionsConnector(SchemaTableName table, TupleDomain<String> tupleDomain)
    {
        return null;
    }
}
