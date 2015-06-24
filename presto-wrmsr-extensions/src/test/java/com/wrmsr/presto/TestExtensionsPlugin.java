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
import com.wrmsr.presto.functions.TypeRegistrar;
import com.wrmsr.presto.util.Box;
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

        return TypeRegistrar.RowTypeConstructorCompiler.blockBuilderToSlice(blockBuilder);
    }

    @Test
    public void testNewThing() throws Throwable
    {
        Slice slice = newThing(0, Slices.wrappedBuffer((byte) 10, (byte) 20), 10, Slices.wrappedBuffer((byte) 30, (byte) 40));
        Block block = new VariableWidthBlockEncoding().readBlock(slice.getInput());
        System.out.println(block);

        RowType rt = new RowType(parameterizedTypeName("thing"), ImmutableList.of(BigintType.BIGINT, VarbinaryType.VARBINARY, BigintType.BIGINT, VarbinaryType.VARBINARY, BooleanType.BOOLEAN, DoubleType.DOUBLE), Optional.of(ImmutableList.of("a", "b", "c", "d", "e", "f")));
        Class<?> cls = new TypeRegistrar.RowTypeConstructorCompiler().run(rt);

        try {
            Method m = cls.getMethod(rt.getTypeSignature().getBase(), long.class, Slice.class, long.class, Slice.class, boolean.class, double.class);
            Object ret = m.invoke(null, 5L, Slices.wrappedBuffer((byte) 10, (byte) 20), 10, null, true, 40.5);// (null, 2L, null, 3L, null);
            System.out.println(ret);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        rt = new RowType(parameterizedTypeName("thing"), ImmutableList.of(BigintType.BIGINT, VarbinaryType.VARBINARY, BigintType.BIGINT, VarbinaryType.VARBINARY, BooleanType.BOOLEAN, DoubleType.DOUBLE), Optional.of(ImmutableList.of("a", "b", "c", "d", "e", "f")));
        cls = new TypeRegistrar.NullableRowTypeConstructorCompiler().run(rt);

        try {
            Method m = cls.getMethod(rt.getTypeSignature().getBase(), Long.class, Slice.class, Long.class, Slice.class, Boolean.class, Double.class);
            Object ret = m.invoke(null, 5L, Slices.wrappedBuffer((byte) 10, (byte) 20), new Long(10), null, new Boolean(true), new Double(40.5));// (null, 2L, null, 3L, null);
            System.out.println(ret);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Test
    public void testBoxes() throws Throwable
    {
        RowType rowType = new RowType(parameterizedTypeName("thing"), ImmutableList.of(BigintType.BIGINT, VarbinaryType.VARBINARY, BigintType.BIGINT, VarbinaryType.VARBINARY, BooleanType.BOOLEAN, DoubleType.DOUBLE), Optional.of(ImmutableList.of("a", "b", "c", "d", "e", "f")));

        /*
        List<RowType.RowField> fieldTypes = rowType.getFields();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(rowType.getTypeSignature().getBase() + "$serializer")
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), "serialize", type(Slice.class), ImmutableList.of(arg("obj", rowType.getJavaType()));

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        com.facebook.presto.byteCode.Block body = methodDefinition.getBody();

        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        */

        // Class<?> cls = TypeRegistrar.generateBox(rowType.getTypeSignature().getBase());
        // cls.getDeclaredConstructor(Slice.class).newInstance(new Object[]{null});
        // System.out.println(cls);
    }
}
