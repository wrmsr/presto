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
package com.wrmsr.presto.metaconnectors;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.mysql.MySqlPlugin;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.metaconnectors.MetaconnectorsPlugin;
import com.wrmsr.presto.metaconnectors.splitter.SplitterConnectorFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public class TestMetaconnectorsPlugin
        extends AbstractTestQueryFramework
{
    public TestMetaconnectorsPlugin()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testSanity()
            throws Exception
    {
        // queryRunner.execute("select * from split_tpch.tiny.lineitem limit 10");
        // queryRunner.execute("select * from split-tcph.lineitem inner join orders on orders.orderkey = lineitem.orderkey inner join customer on orders.custkey = customer.custkey limit 10");
        MaterializedResult r;

        /*
        r = queryRunner.execute("select max(id) from split_yelp.yelp.business");
        System.out.println(r);

        r = queryRunner.execute("select count(*) from split_yelp.yelp.business");
        System.out.println(r);
        */

        r = queryRunner.execute("select * from split_yelp.yelp.business where id between 12950000 and 12950005");
        System.out.println(r);
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        LocalQueryRunner queryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        queryRunner.createCatalog(
                defaultSession.getCatalog(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        /*
        TpchPlugin tpchPlugin = new TpchPlugin();
        tpchPlugin.setNodeManager(queryRunner.getNodeManager());
        for (ConnectorFactory connectorFactory: tpchPlugin.getServices(ConnectorFactory.class)) {
            queryRunner.getConnectorManager().addConnectorFactory(connectorFactory);
        }
        */

        MySqlPlugin mySqlPlugin = new MySqlPlugin();
        for (ConnectorFactory connectorFactory : mySqlPlugin.getServices(ConnectorFactory.class)) {
            queryRunner.getConnectorManager().addConnectorFactory(connectorFactory);
        }

        MetaconnectorsPlugin plugin = new MetaconnectorsPlugin();
        plugin.setConnectorManager(queryRunner.getConnectorManager());
        plugin.setNodeManager(queryRunner.getNodeManager());
        plugin.setTypeManager(queryRunner.getTypeManager());

        for (ConnectorFactory connectorFactory : plugin.getServices(ConnectorFactory.class)) {
            queryRunner.getConnectorManager().addConnectorFactory(connectorFactory);
            if (connectorFactory instanceof SplitterConnectorFactory) {
                Map<String, String> properties = ImmutableMap.<String, String>builder()
                        .put("target-name", "yelp")
                        .put("target-connector-name", "mysql")
                        .put("target.connection-url", "jdbc:mysql://proddb3-r1-devc:3306")
                        .put("target.connection-user", "wtimoney")
                        .put("target.connection-password", "")
                        .build();

                queryRunner.getConnectorManager().createConnection("split_yelp", "splitter", properties);
            }
        }

        /*
        for (Type type : plugin.getServices(Type.class)) {
            queryRunner.getTypeManager().addType(type);
        }
        for (ParametricType parametricType : plugin.getServices(ParametricType.class)) {
            queryRunner.getTypeManager().addParametricType(parametricType);
        }
        */
        // queryRunner.getMetadata().getFunctionRegistry().addFunctions(Iterables.getOnlyElement(plugin.getServices(FunctionFactory.class)).listFunctions());

        return queryRunner;
    }
}
