/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcPartition;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.jdbc.util.ScriptRunner;
import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Files.downloadFile;
import static com.wrmsr.presto.util.Jvm.addClasspathUrl;

public class ExtendedJdbcClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ExtendedJdbcClient.class);

    protected final ExtendedJdbcConfig extendedConfig;

    public ExtendedJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, ExtendedJdbcConfig extendedConfig, String identifierQuote, Driver driver)
    {
        super(connectorId, config, identifierQuote, driver);
        this.extendedConfig = extendedConfig;
    }

    @Override
    public Connection getConnection(String url, Properties info)
            throws SQLException
    {
        if (driver != null) {
            return driver.connect(url, info);
        }
        else {
            return DriverManager.getConnection(url, info);
        }
    }

    public Connection getConnection()
            throws SQLException
    {
        return getConnection(connectionUrl, connectionProperties);
    }

    public void executeScript(String sql)
    {
        try (Connection connection = getConnection(connectionUrl, connectionProperties)) {
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.runScript(new StringReader(sql));
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    public boolean isRemotelyAccessible()
    {
        return extendedConfig.getIsRemotelyAccessible();
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        ExtendedJdbcSplit jdbcSplit = new ExtendedJdbcSplit(
                connectorId,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                connectionUrl,
                fromProperties(connectionProperties),
                jdbcPartition.getTupleDomain(),
                isRemotelyAccessible());
        return new FixedSplitSource(connectorId, ImmutableList.of(jdbcSplit));
    }

    private static final Map<String, File> downloadedDrivers = newHashMap();

    public static Driver createDriver(ExtendedJdbcConfig extendedConfig, Supplier<Driver> supplier)
    {
        String driverUrl = extendedConfig.getDriverUrl();
        if (!isNullOrEmpty(driverUrl)) {
            try {
                synchronized (downloadedDrivers) {
                    File jarPath;
                    if (downloadedDrivers.containsKey(driverUrl)) {
                        jarPath = downloadedDrivers.get(driverUrl);
                    }
                    else {
                        File tempPath = Files.createTempDirectory("temp").toFile();
                        tempPath.deleteOnExit();
                        jarPath = new File(tempPath, "driver.jar");
                        log.info(String.format("Downloading driver from %s to %s", driverUrl, jarPath));
                        downloadFile(driverUrl, jarPath);
                        downloadedDrivers.put(driverUrl, jarPath);
                    }
                    addClasspathUrl(Thread.currentThread().getContextClassLoader(), jarPath.toURL());
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        String driverClass = extendedConfig.getDriverClass();
        if (!isNullOrEmpty(driverClass)) {
            return newDriver(driverClass);
        }

        return supplier.get();
    }

    @SuppressWarnings({"unchecked"})
    public static Driver newDriver(String fqcn)
    {
        Class<? extends Driver> cls;
        try {
           cls = (Class<? extends Driver>) Class.forName(fqcn);
        }
        catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
        return newDriver(cls);
    }

    public static Driver newDriver(Class<? extends Driver> cls)
    {
        try {
            return cls.getDeclaredConstructor().newInstance();
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw Throwables.propagate(e);
        }
    }

    public String quoted(String name)
    {
        return super.quoted(name);
    }

    public String quoted(String catalog, String schema, String table)
    {
        return super.quoted(catalog, schema, table);
    }
}
