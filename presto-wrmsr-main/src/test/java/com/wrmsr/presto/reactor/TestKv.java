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
package com.wrmsr.presto.reactor;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Maps.fromProperties;
import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Locale.ENGLISH;

public class TestKv
{
    public interface ConnectorEventSource
    {

    }

    @Test
    public void test()
            throws Throwable
    {
        /*
        final Session session = Session.builder(new SessionPropertyManager())
                .setUser("user")
                .setSource("test")
                .setCatalog("default")
                .setSchema("default")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();
        */

        final ConnectorSession session = new TestingConnectorSession(
                "user",
                UTC_KEY,
                ENGLISH,
                System.currentTimeMillis(),
                ImmutableList.of(),
                ImmutableMap.of());

        JdbcConnectorFactory connectorFactory = new JdbcConnectorFactory(
                "test",
                new TestingH2JdbcModule(),
                ImmutableMap.<String, String>of(),
                getClass().getClassLoader());

        File tmp = Files.createTempDir();
        tmp.deleteOnExit();
        File db = new File(tmp, "db");
        Connector connector = connectorFactory.create("test", TestingH2JdbcModule.createProperties(db));
        // connector.getMetadata().createTable(session,
        ConnectorMetadata metadata = connector.getMetadata();
        //new JdbcMetadata(new JdbcConnectorId(CONNECTOR_ID), database.getJdbcClient(), new JdbcMetadataConfig());

        JdbcMetadata jdbcMetadata = (JdbcMetadata) metadata;
        BaseJdbcClient jdbcClient = (BaseJdbcClient) jdbcMetadata.getJdbcClient();
        try (Connection connection = jdbcClient.getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE SCHEMA example");
            }
            // connection.createStatement().execute("CREATE TABLE example.foo (id integer primary key)");
        }

        ConnectorOutputTableHandle oth = metadata.beginCreateTable(session, new ConnectorTableMetadata(
                new SchemaTableName("example", "foo"),
                ImmutableList.of(new ColumnMetadata("text", VARCHAR, false)),
                ImmutableMap.of(),
                "bob"));
        metadata.commitCreateTable(session, oth, ImmutableList.of());

        try (Connection connection = jdbcClient.getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("insert into db.example.foo (text) values ('hi1');");
            }
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("insert into \"DB\".\"EXAMPLE\".\"FOO\" (text) values ('hi2');");
            }
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("select * from db.example.foo;");
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
                System.out.println();
            }
            /*
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("select * from information_schema.tables;");
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    System.out.println(rs.getString(3));
                    System.out.println();
                }
            }
            */
            connection.commit();
            // connection.createStatement().execute("CREATE TABLE example.foo (id integer primary key)");
        }
        /*
        ConnectorTableHandle th = metadata.getTableHandle(session, new SchemaTableName("example", "foo"));
        ConnectorInsertTableHandle ith = metadata.beginInsert(session, th);
        metadata.commitInsert(session, ith, );
        */

        ConnectorTableHandle th = metadata.getTableHandle(session, new SchemaTableName("EXAMPLE", "FOO"));
        Map<String, ColumnHandle> m = metadata.getColumnHandles(session, th);
        List<ColumnMetadata> cms = m.values().stream().map(h -> metadata.getColumnMetadata(session, th, h)).collect(toImmutableList());

        oth = new JdbcOutputTableHandle(
                "test",
                "DB",
                "EXAMPLE",
                "FOO",
                cms.stream().map(ColumnMetadata::getName).collect(toImmutableList()),
                cms.stream().map(ColumnMetadata::getType).collect(toImmutableList()),
                "BOB",
                "FOO",
                jdbcClient.getConnectionUrl(),
                fromProperties(jdbcClient.getConnectionProperties()));

        RecordSink rs = connector.getRecordSinkProvider().getRecordSink(session, oth);

        rs.beginRecord(1);
        rs.appendString("hi3".getBytes());
        rs.finishRecord();

        rs.commit();

        try (Connection connection = jdbcClient.getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("select * from DB.EXAMPLE.FOO;");
                ResultSet rs2 = stmt.getResultSet();
                while (rs2.next()) {
                    System.out.println(rs2.getString(1));
                }
                System.out.println();
            }
        }
    }
}
