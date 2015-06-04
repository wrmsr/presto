package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FlatColumnHandle extends ConnectorIdOnlyHandle
    implements ColumnHandle
{
    @JsonCreator
    public FlatColumnHandle(
            @JsonProperty("connectorId") String connectorId)
    {
        super(connectorId);
    }
}
