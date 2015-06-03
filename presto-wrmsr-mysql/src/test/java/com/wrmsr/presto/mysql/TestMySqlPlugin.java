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
package com.wrmsr.presto.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;

public class TestMySqlPlugin
        extends AbstractTestQueryFramework
{
    public TestMySqlPlugin()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testSanity()
            throws Exception
    {
        queryRunner.execute("select * from lineitem inner join orders on orders.orderkey = lineitem.orderkey inner join customer on orders.custkey = customer.custkey limit 10");
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

        ExtendedMySqlPlugin plugin = new ExtendedMySqlPlugin();
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
