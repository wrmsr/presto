package com.wrmsr.presto.connectorSupport;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.reactor.tuples.PkLayout;

import java.util.List;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public abstract class ConnectorSupport<C extends Connector>
{
    protected final Session session;
    protected final C connector;

    public ConnectorSupport(Session session, C connector)
    {
        this.session = session;
        this.connector = connector;
    }

    public C getConnector()
    {
        return connector;
    }

    public abstract SchemaTableName getSchemaTableName(ConnectorTableHandle handle);

    public abstract String getColumnName(ColumnHandle columnHandle);

    public abstract Type getColumnType(ColumnHandle columnHandle);

    public abstract List<String> getPrimaryKey(SchemaTableName schemaTableName);

    public PkLayout<String> getTableTupleLayout(SchemaTableName schemaTableName)
    {
        List<String> pk = getPrimaryKey(schemaTableName);
        ConnectorSession cs = session.toConnectorSession();
        ConnectorMetadata m = connector.getMetadata();
        ConnectorTableHandle th = m.getTableHandle(cs, schemaTableName);
        List<ColumnHandle> chs = m.getColumnHandles(cs, th).values().stream().collect(toImmutableList());
        return new PkLayout<>(
                chs.stream().map(this::getColumnName).collect(toImmutableList()),
                chs.stream().map(this::getColumnType).collect(toImmutableList()),
                pk);
    }

    // exec
}
