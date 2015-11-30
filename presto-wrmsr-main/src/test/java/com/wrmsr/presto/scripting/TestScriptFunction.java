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
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.ParametricType;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
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
    public void testStuff() throws Throwable
    {
        LocalQueryRunner runner = createLocalQueryRunner();

        assertQuery("select script('a', 'b', 1, 'two')", "select null");
    }

    private buildThunk()
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(rowType.getDisplayName() + "_new"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        List<Parameter> parameters = createParameters(fieldTypes);

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(com.facebook.presto.spi.block.Block.class), parameters);
        methodDefinition.declareAnnotation(ScalarFunction.class);
        methodDefinition.declareAnnotation(SqlType.class).setValue("value", rowType.getTypeSignature().toString());
        for (int i = 0; i < fieldTypes.size(); i++) {
            methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().toString());
        }

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        com.facebook.presto.byteCode.ByteCodeBlock body = methodDefinition.getBody();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");

        body
                .newObject(type(VariableWidthBlockBuilder.class, BlockBuilderStatus.class))
                .dup()
                .dup()
                .newObject(BlockBuilderStatus.class)
                .dup()
                .invokeConstructor(BlockBuilderStatus.class)
                .invokeConstructor(VariableWidthBlockBuilder.class, BlockBuilderStatus.class)
                .putVariable(blockBuilder);

        // FIXME: reuse returned blockBuilder

        for (int i = 0; i < fieldTypes.size(); i++) {
            Variable arg = scope.getVariable("arg" + i);
            Class<?> javaType = fieldTypes.get(i).getType().getJavaType();

            if (javaType == boolean.class) {
                writeBoolean(body, blockBuilder, arg, i);
            }
            else if (javaType == long.class) {
                writeLong(body, blockBuilder, arg, i);
            }
            else if (javaType == double.class) {
                writeDouble(body, blockBuilder, arg, i);
            }
            else if (javaType == Slice.class) {
                writeSlice(body, blockBuilder, arg, i);
            }
            else if (javaType == com.facebook.presto.spi.block.Block.class) {
                writeObject(body, blockBuilder, arg, i);
            }
            else {
                throw new IllegalArgumentException("bad value: " + javaType);
            }
        }

        body
                .getVariable(blockBuilder)
                .invokeInterface(BlockBuilder.class, "build", com.facebook.presto.spi.block.Block.class)
                .retObject();

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(RowTypeConstructorCompiler.class.getClassLoader()));
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

        ScriptingManager scriptingManager = new ScriptingManager();
        List<? extends SqlFunction> functions = IntStream.range(1, 3).boxed().map(i -> new ScriptFunction(scriptingManager, VarcharType.VARCHAR, i)).collect(toImmutableList());
        localQueryRunner.getMetadata().getFunctionRegistry().addFunctions(functions);
        return localQueryRunner;
    }
}
