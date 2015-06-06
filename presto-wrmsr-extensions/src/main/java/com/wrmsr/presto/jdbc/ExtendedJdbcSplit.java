package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Map;

public class ExtendedJdbcSplit
    extends JdbcSplit
{
    private final boolean isRemotelyAccessible;

    @JsonCreator
    public ExtendedJdbcSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("connectionUrl") String connectionUrl,
            @JsonProperty("connectionProperties") Map<String, String> connectionProperties,
            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
            @JsonProperty("isRemotelyAccessible") boolean isRemotelyAccessible)
    {
        super(connectorId, catalogName, schemaName, tableName, connectionUrl, connectionProperties, tupleDomain);
        this.isRemotelyAccessible = isRemotelyAccessible;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return isRemotelyAccessible;
    }
}
