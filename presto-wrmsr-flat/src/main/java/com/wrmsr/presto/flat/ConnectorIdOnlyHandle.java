package com.wrmsr.presto.flat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ConnectorIdOnlyHandle
{
    private final String connectorId;

    @JsonCreator
    public ConnectorIdOnlyHandle(
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
        ConnectorIdOnlyHandle other = (ConnectorIdOnlyHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId);
    }

    @Override
    public String toString()
    {
        return connectorId;
    }
}
