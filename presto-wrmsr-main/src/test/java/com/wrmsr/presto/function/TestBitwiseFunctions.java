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
package com.wrmsr.presto.function;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.function.bitwise.BitAndAggregationFunction;
import com.wrmsr.presto.function.bitwise.BitAndFunction;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestBitwiseFunctions
        extends AbstractTestQueryFramework
{

    public TestBitwiseFunctions()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testStuff()
            throws Throwable
    {
        assertQuery("select bit_and_agg(n) from (select 111 n union all select 222 n)", "select 0");
        assertQuery("select bit_and(111, 222)", "select 0");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(localQueryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(
                new FunctionListBuilder(localQueryRunner.getTypeManager())
                        .aggregate(BitAndAggregationFunction.class)
                        .function(BitAndFunction.BIT_AND_FUNCTION)
                        .getFunctions());
        return localQueryRunner;
    }
}
