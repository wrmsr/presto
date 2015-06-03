package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class FlatTableHandle
    implements ConnectorTableHandle
{
    private final String connectorId;

    @JsonCreator
    public FlatTableHandle(
            @JsonProperty("connectorId") String connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlatTableHandle other = (FlatTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
