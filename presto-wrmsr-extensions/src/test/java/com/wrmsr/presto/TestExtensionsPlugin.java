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
package com.wrmsr.presto;

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.*;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.*;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.RowType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.ExtensionsPlugin;
import io.airlift.slice.BasicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.byteCode.Access.*;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Locale.ENGLISH;

public class TestExtensionsPlugin
        extends AbstractTestQueryFramework
{
    public TestExtensionsPlugin()
    {
        super(createLocalQueryRunner());
    }

    @Test
    public void testSanity()
            throws Exception
    {
        queryRunner.execute("select * from lineitem inner join orders on orders.orderkey = lineitem.orderkey inner join customer on orders.custkey = customer.custkey limit 10");
    }

    @Test
    public void testTypeStuff()
        throws Throwable
    {
        Type rt = new RowType(
                parameterizedTypeName("thing"), ImmutableList.<Type>of(DoubleType.DOUBLE, BigintType.BIGINT), Optional.of(ImmutableList.of("a", "b")));
        System.out.println(rt);
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
       ExtensionsPlugin plugin = new ExtensionsPlugin();
       plugin.setTypeRegistry(queryRunner.getTypeManager());
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

    public Slice newThing(long a, Slice b, long c, Slice d)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus());

        blockBuilder.writeLong(a);
        blockBuilder.closeEntry();

        blockBuilder.writeBytes(b, 0, b.length());
        blockBuilder.closeEntry();

        blockBuilder.writeLong(c);
        blockBuilder.closeEntry();

        blockBuilder.writeBytes(d, 0, d.length());
        blockBuilder.closeEntry();

        return blockBuilderToSlice(blockBuilder);
    }

    public static Slice blockBuilderToSlice(BlockBuilder blockBuilder)
    {
        return blockToSlice(blockBuilder.build());
    }

    public static Slice blockToSlice(Block block)
    {
        BlockEncoding blockEncoding = new VariableWidthBlockEncoding();

        int estimatedSize = blockEncoding.getEstimatedSize(block);
        Slice outputSlice = Slices.allocate(estimatedSize);
        SliceOutput sliceOutput = outputSlice.getOutput();

        blockEncoding.writeBlock(sliceOutput, block);
        checkState(sliceOutput.size() == estimatedSize);

        return outputSlice;
    }

    public void generateConstructor(RowType rowType)
    {
        List<RowType.RowField> fieldTypes = rowType.getFields();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(rowType.getDisplayName() + "_new"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowType.RowField fieldType = fieldTypes.get(i);
            parameters.add(arg("arg" + i, fieldType.getType().getJavaType()));
        }

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), "_new", type(Slice.class), parameters.build());
        Scope scope = methodDefinition.getScope();

        CallSiteBinder binder = new CallSiteBinder();
        com.facebook.presto.byteCode.Block body = methodDefinition.getBody();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        body
                .comment("blockBuilder = typeVariable.createBlockBuilder(new BlockBuilderStatus());")
                .newObject(BlockBuilderStatus.class)
                .dup()
                .invokeConstructor(BlockBuilderStatus.class)
                .newObject(type(VariableWidthBlockBuilder.class, BlockBuilderStatus.class))
                .invokeConstructor(VariableWidthBlockBuilder.class, BlockBuilderStatus.class)
                .putVariable(blockBuilder);

        // FIXME: reuse returned blockBuilder

        for (int i = 0; i < fieldTypes.size(); i++) {
            Variable arg = scope.getVariable("arg" + i);
            Class<?> javaType = fieldTypes.get(i).getType().getJavaType();
            LabelNode isNull = new LabelNode("isNull" + i);

            body
                    .getVariable(arg)
                    .ifNullGoto(isNull);

            if (javaType == boolean.class) {
                // type.writeBoolean(builder, (Boolean) value);
            }
            else if (javaType == long.class) {
                body
                        .getVariable(blockBuilder)
                        .getVariable(arg)
                        .invokeInterface(BlockBuilder.class, "writeLong", BlockBuilder.class, long.class)
                        .pop();
            }
            else if (javaType == double.class) {
                body
                        .getVariable(blockBuilder)
                        .getVariable(arg)
                        .invokeInterface(BlockBuilder.class, "writeDouble", BlockBuilder.class, double.class)
                        .pop();
            }
            else if (javaType == Slice.class) {
                body
                        .getVariable(blockBuilder)
                        .push(0L)
                        .getVariable(blockBuilder)
                        .invokeVirtual(Slice.class, "length", Slice.class)
                        .invokeInterface(BlockBuilder.class, "writeBytes", BlockBuilder.class, Slice.class)
                        .pop();
            }
            else {
                throw new IllegalArgumentException("bad value: " + javaType);
            }

            LabelNode done = new LabelNode("done" + i);
            body
                    .gotoLabel(done)
                    .visitLabel(isNull)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .visitLabel(done)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
        }

        body
                .getVariable(blockBuilder)
                .invokeStatic(TestExtensionsPlugin.class, "blockBuilderToSlice", TestExtensionsPlugin.class)
                .retObject();


        Class<?> cls = defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(TestExtensionsPlugin.class.getClassLoader()));

        try {
            cls.getDeclaredMethod("_new");
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }


    @Test
    public void testNewThing() throws Throwable
    {
        Slice slice = newThing(0, Slices.wrappedBuffer((byte) 10, (byte) 20), 10, Slices.wrappedBuffer((byte) 30, (byte) 40));
        Block block = new VariableWidthBlockEncoding().readBlock(slice.getInput());
        System.out.println(block);

        RowType rt = new RowType(parameterizedTypeName("thing"), ImmutableList.of(BigintType.BIGINT, VarbinaryType.VARBINARY, BigintType.BIGINT, VarbinaryType.VARBINARY), Optional.of(ImmutableList.of("a", "b", "c", "d")));
        generateConstructor(rt);
    }
}
