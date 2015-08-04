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
import com.facebook.presto.plugin.jdbc.JdbcConnectorFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.util.Locale.ENGLISH;

public class TestCrud
{
    public interface CrudConnectorAdapter
    {
        List select();

        void insert();

        void update();

        void delete();
    }

    public class CrudConnectorAdapterImpl implements CrudConnectorAdapter
    {
        @Override
        public List select()
        {
            return null;
        }

        @Override
        public void insert()
        {

        }

        @Override
        public void update()
        {

        }

        @Override
        public void delete()
        {

        }
    }

    @Test
    public void test() throws Throwable
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

        Connector connector = connectorFactory.create("test", TestingH2JdbcModule.createProperties());
        // connector.getMetadata().createTable(session,
        ConnectorMetadata metadata = connector.getMetadata();
        //new JdbcMetadata(new JdbcConnectorId(CONNECTOR_ID), database.getJdbcClient(), new JdbcMetadataConfig());

        metadata.createTable(session, new ConnectorTableMetadata(
                new SchemaTableName("example", "foo"),
                ImmutableList.of(new ColumnMetadata("text", VARCHAR, false))));
    }
}
