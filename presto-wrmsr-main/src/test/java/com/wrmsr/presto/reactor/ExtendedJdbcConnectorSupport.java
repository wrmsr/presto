package com.wrmsr.presto.reactor;

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnectorFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ExtendedJdbcConnectorSupport
        extends ConnectorSupport<ExtendedJdbcConnector>
{
    public ExtendedJdbcConnectorSupport(ExtendedJdbcConnector connector)
    {
        super(connector);
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
}
