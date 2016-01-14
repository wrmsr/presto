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
package com.wrmsr.presto.scripting;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class TestScriptFunction
        extends AbstractTestQueryFramework
{

    public TestScriptFunction()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testStuff()
            throws Throwable
    {
        LocalQueryRunner runner = createLocalQueryRunner();

        assertQuery("select script('a', 'b', null, 'two')", "select null");
        assertQuery("select script('a', 'b', 1, 'two')", "select null");
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

        ScriptingManager scriptingManager = new ScriptingManager(null, ImmutableSet.of(), ImmutableList.of());
        List<? extends SqlFunction> functions = IntStream.range(1, 3).boxed().map(i -> new ScriptFunction(scriptingManager, new ScriptFunction.Config("script", VarcharType.VARCHAR, i, ScriptFunction.ExecutionType.INVOKE))).collect(toImmutableList());
        localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(functions);
        return localQueryRunner;
    }
}
