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

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.wrmsr.presto.jdbc.util.ScriptRunner;
import com.wrmsr.presto.util.Configs;
import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
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
    public Connection getConnection(String url, Properties info) throws SQLException
    {
        if (driver != null) {
            return driver.connect(url, info);
        }
        else {
            return DriverManager.getConnection(url, info);
        }
    }

    public void runInitScripts()
    {
        for (String sql : extendedConfig.getInitScripts()) {
            executeScript(sql);
        }
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
            try {
                Class.forName(driverClass);
            }
            catch (ClassNotFoundException e) {
                throw Throwables.propagate(e);
            }
        }

        return supplier.get();
    }
}
