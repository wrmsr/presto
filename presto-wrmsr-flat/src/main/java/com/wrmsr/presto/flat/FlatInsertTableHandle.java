package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FlatInsertTableHandle extends ConnectorIdOnlyHandle
    implements ConnectorInsertTableHandle
{
    public FlatInsertTableHandle(@JsonProperty("connectorId") String connectorId) {
        super(connectorId);
    }
}
