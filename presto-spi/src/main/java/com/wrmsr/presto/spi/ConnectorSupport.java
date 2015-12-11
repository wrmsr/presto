package com.wrmsr.presto.spi;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public abstract class ConnectorSupport<C extends Connector>
{
    protected final ConnectorSession connectorSession;
    protected final C connector;

    public ConnectorSupport(ConnectorSession connectorSession, C connector)
    {
        this.connectorSession = connectorSession;
        this.connector = connector;
    }

    public ConnectorSession getConnectorSession()
    {
        return connectorSession;
    }

    public C getConnector()
    {
        return connector;
    }

    public abstract SchemaTableName getSchemaTableName(ConnectorTableHandle handle);

    public abstract String getColumnName(ColumnHandle columnHandle);

    public abstract Type getColumnType(ColumnHandle columnHandle);

    public List<String> getPrimaryKey(SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException();
    }

    public void exec(String buf)
    {
        throw new UnsupportedOperationException();
    }

    public List eval(String cmd)
    {
        throw new UnsupportedOperationException();
    }

    // getEventSource
}

