package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FlatOutputTableHandle extends ConnectorIdOnlyHandle
    implements ConnectorOutputTableHandle
{
    @JsonCreator
    public FlatOutputTableHandle(
            @JsonProperty("connectorId") String connectorId)
    {
        super(connectorId);
    }
}
