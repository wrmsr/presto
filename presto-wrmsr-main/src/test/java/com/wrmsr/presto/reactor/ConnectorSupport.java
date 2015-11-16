package com.wrmsr.presto.reactor;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnector;

import java.util.List;

public abstract class ConnectorSupport<C extends Connector>
{
    protected final C connector;

    @SuppressWarnings({"unchecked"})
    public ConnectorSupport(C connector)
    {
        this.connector = connector;
    }

    public C getConnector()
    {
        return connector;
    }

    public abstract SchemaTableName getSchemaTableName(ConnectorTableHandle handle);

    public abstract List<String> getPrimaryKey(SchemaTableName schemaTableName);

    public abstract String getColumnName(ColumnHandle columnHandle);
}
