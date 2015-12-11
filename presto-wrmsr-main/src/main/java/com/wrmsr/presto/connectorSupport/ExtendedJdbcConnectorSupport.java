package com.wrmsr.presto.connectorSupport;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.connector.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.spi.ConnectorSupport;
import com.wrmsr.presto.util.jdbc.ScriptRunner;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ExtendedJdbcConnectorSupport
        extends ConnectorSupport<ExtendedJdbcConnector>
{
    public ExtendedJdbcConnectorSupport(ConnectorSession connectorSession, ExtendedJdbcConnector connector)
    {
        super(connectorSession, connector);
    }

    public ExtendedJdbcClient getClient()
    {
        return (ExtendedJdbcClient) connector.getJdbcClient();
    }

    @Override
    public SchemaTableName getSchemaTableName(ConnectorTableHandle handle)
    {
        checkArgument(handle instanceof JdbcTableHandle);
        return ((JdbcTableHandle) handle).getSchemaTableName();
    }

    @Override
    public List<String> getPrimaryKey(SchemaTableName schemaTableName)
    {
        try {
            try (Connection connection = getClient().getConnection()) {
                DatabaseMetaData metadata = connection.getMetaData();

                // FIXME postgres catalog support
                try (ResultSet resultSet = metadata.getPrimaryKeys(schemaTableName.getSchemaName(), schemaTableName.getSchemaName(), schemaTableName.getTableName())) {
                    while (resultSet.next()) {
                        System.out.println(resultSet);
                    }
                }

                throw new UnsupportedOperationException();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getColumnName(ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnName();
    }

    @Override
    public Type getColumnType(ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnType();
    }

    @Override
    public void exec(String buf)
    {
        JdbcMetadata metadata = (JdbcMetadata) connector.getMetadata();
        BaseJdbcClient client = (BaseJdbcClient) metadata.getJdbcClient();
        try (Connection connection = client.getConnection()) {
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.runScript(new StringReader(buf));
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }
}
