package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FlatTableHandle extends ConnectorIdOnlyHandle
    implements ConnectorTableHandle
{
    @JsonCreator
    public FlatTableHandle(
            @JsonProperty("connectorId") String connectorId)
    {
        super(connectorId);
    }
}
