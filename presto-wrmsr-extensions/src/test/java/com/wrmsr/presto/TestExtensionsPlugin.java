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
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.*;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.ExtensionsPlugin;
import io.airlift.slice.BasicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
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

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), rowType.getTypeSignature().getBase(), type(Slice.class), parameters.build());
        methodDefinition.declareAnnotation(ScalarFunction.class);
        methodDefinition.declareAnnotation(SqlType.class).setValue("value", rowType.getDisplayName()); // ???
        for (int i = 0; i < fieldTypes.size(); i++) {
            RowType.RowField fieldType = fieldTypes.get(i);
            methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().getDisplayName());
        }
        Scope scope = methodDefinition.getScope();

        CallSiteBinder binder = new CallSiteBinder();
        com.facebook.presto.byteCode.Block body = methodDefinition.getBody();

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
                LabelNode isFalse = new LabelNode("isFalse" + i);
                LabelNode done = new LabelNode("done" + i);
                body
                        .getVariable(blockBuilder)
                        .getVariable(arg)
                        .ifFalseGoto(isFalse)
                        .push(1)
                        .gotoLabel(done)
                        .visitLabel(isFalse)
                        .push(0)
                        .visitLabel(done)
                        .invokeInterface(BlockBuilder.class, "writeByte", BlockBuilder.class, int.class)
                        .pop();
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
                LabelNode isNull = new LabelNode("isNull" + i);
                LabelNode done = new LabelNode("done" + i);
                body
                        .getVariable(arg)
                        .ifNullGoto(isNull)
                        .getVariable(blockBuilder)
                        .getVariable(arg)
                        .push(0)
                        .getVariable(arg)
                        .invokeVirtual(Slice.class, "length", int.class)
                        .invokeInterface(BlockBuilder.class, "writeBytes", BlockBuilder.class, Slice.class, int.class, int.class)
                        .pop()
                        .gotoLabel(done)
                        .visitLabel(isNull)
                        .getVariable(blockBuilder)
                        .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                        .pop()
                        .visitLabel(done);
            }
            else {
                throw new IllegalArgumentException("bad value: " + javaType);
            }

            body
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop();
        }

        body
                .getVariable(blockBuilder)
                .invokeStatic(TestExtensionsPlugin.class, "blockBuilderToSlice", Slice.class, BlockBuilder.class)
                .retObject();

        Class<?> cls = defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(TestExtensionsPlugin.class.getClassLoader()));

        try {
            Method m = cls.getMethod(rowType.getTypeSignature().getBase(), long.class, Slice.class, long.class, Slice.class, boolean.class, double.class);
            Object ret = m.invoke(null, 5L, Slices.wrappedBuffer((byte) 10, (byte) 20), 10, null, true, 40.5);// (null, 2L, null, 3L, null);
            System.out.println(ret);
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

        RowType rt = new RowType(parameterizedTypeName("thing"), ImmutableList.of(BigintType.BIGINT, VarbinaryType.VARBINARY, BigintType.BIGINT, VarbinaryType.VARBINARY, BooleanType.BOOLEAN, DoubleType.DOUBLE), Optional.of(ImmutableList.of("a", "b", "c", "d", "e", "f")));
        generateConstructor(rt);
    }
}
