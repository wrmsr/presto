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
package com.wrmsr.presto.functions;

import com.facebook.presto.Session;
import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticErrorCode;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.wrmsr.presto.util.Box;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public class StructManager
{
    private final TypeRegistry typeRegistry;
    private final Metadata metadata;

    public StructManager(TypeRegistry typeRegistry, Metadata metadata)
    {
        this.typeRegistry = typeRegistry;
        this.metadata = checkNotNull(metadata);
    }

    /*
    */

    public static class RowTypeConstructorCompiler
    {
        protected List<Parameter> createParameters(List<RowType.RowField> fieldTypes)
        {
            ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
            for (int i = 0; i < fieldTypes.size(); i++) {
                RowType.RowField fieldType = fieldTypes.get(i);
                parameters.add(arg("arg" + i, fieldType.getType().getJavaType()));
            }
            return parameters.build();
        }

        protected void annotateParameters(List<RowType.RowField> fieldTypes, MethodDefinition methodDefinition)
        {
            for (int i = 0; i < fieldTypes.size(); i++) {
                methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().toString());
            }
        }

        protected void writeBoolean(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
        {
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
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop();
        }

        protected void writeLong(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
        {
            body
                    .getVariable(blockBuilder)
                    .getVariable(arg)
                    .invokeInterface(BlockBuilder.class, "writeLong", BlockBuilder.class, long.class)
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop();
        }

        protected void writeDouble(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
        {
            body
                    .getVariable(blockBuilder)
                    .getVariable(arg)
                    .invokeInterface(BlockBuilder.class, "writeDouble", BlockBuilder.class, double.class)
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop();
        }

        protected void writeSlice(com.facebook.presto.byteCode.Block body, Variable blockBuilder, Variable arg, int i)
        {
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

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop()

                    .gotoLabel(done)
                    .visitLabel(isNull)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .visitLabel(done);
        }

        public Class<?> run(RowType rowType)
        {
            return run(rowType, rowType.getTypeSignature().getBase());
        }

        public Class<?> run(RowType rowType, String name)
        {
            // TODO foo_array
            List<RowType.RowField> fieldTypes = rowType.getFields();

            ClassDefinition definition = new ClassDefinition(
                    a(PUBLIC, FINAL),
                    CompilerUtils.makeClassName(rowType.getDisplayName() + "_new"),
                    type(Object.class));

            definition.declareDefaultConstructor(a(PRIVATE));

            List<Parameter> parameters = createParameters(fieldTypes);

            MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(Slice.class), parameters);
            methodDefinition.declareAnnotation(ScalarFunction.class);
            methodDefinition.declareAnnotation(SqlType.class).setValue("value", rowType.getTypeSignature().toString());
            annotateParameters(fieldTypes, methodDefinition);

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
                else {
                    throw new IllegalArgumentException("bad value: " + javaType);
                }
            }

            body
                    .getVariable(blockBuilder)
                    .invokeStatic(RowTypeConstructorCompiler.class, "blockBuilderToSlice", Slice.class, BlockBuilder.class)
                    .retObject();

            return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(RowTypeConstructorCompiler.class.getClassLoader()));
        }

        public static Slice blockBuilderToSlice(BlockBuilder blockBuilder)
        {
            return blockToSlice(blockBuilder.build());
        }

        public static Slice blockToSlice(com.facebook.presto.spi.block.Block block)
        {
            BlockEncoding blockEncoding = new VariableWidthBlockEncoding();

            int estimatedSize = blockEncoding.getEstimatedSize(block);
            Slice outputSlice = Slices.allocate(estimatedSize);
            SliceOutput sliceOutput = outputSlice.getOutput();

            blockEncoding.writeBlock(sliceOutput, block);
            checkState(sliceOutput.size() == estimatedSize);

            return outputSlice;
        }
    }

    public static class NullableRowTypeConstructorCompiler extends RowTypeConstructorCompiler
    {
        @Override
        protected List<Parameter> createParameters(List<RowType.RowField> fieldTypes)
        {
            ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
            for (int i = 0; i < fieldTypes.size(); i++) {
                RowType.RowField fieldType = fieldTypes.get(i);
                Class<?> javaType = fieldType.getType().getJavaType();
                if (javaType == boolean.class) {
                    javaType = Boolean.class;
                }
                else if (javaType == long.class) {
                    javaType = Long.class;
                }
                else if (javaType == double.class) {
                    javaType = Double.class;
                }
                else if (javaType == Slice.class) {
                    // nop
                }
                else {
                    throw new IllegalArgumentException("javaType: " + javaType.toString());
                }
                parameters.add(arg("arg" + i, javaType));
            }
            return parameters.build();
        }

        @Override
        protected void annotateParameters(List<RowType.RowField> fieldTypes, MethodDefinition methodDefinition)
        {
            for (int i = 0; i < fieldTypes.size(); i++) {
                methodDefinition.declareParameterAnnotation(Nullable.class, i);
                methodDefinition.declareParameterAnnotation(SqlType.class, i).setValue("value", fieldTypes.get(i).getType().toString());
            }
        }

        @Override
        protected void writeBoolean(Block body, Variable blockBuilder, Variable arg, int i)
        {
            LabelNode isNull = new LabelNode("isNull" + i);
            LabelNode isFalse = new LabelNode("isFalse" + i);
            LabelNode write = new LabelNode("write" + i);
            LabelNode done = new LabelNode("done" + i);
            body
                    .getVariable(arg)
                    .ifNullGoto(isNull)
                    .getVariable(blockBuilder)

                    .getVariable(arg)
                    .invokeVirtual(Boolean.class, "booleanValue", boolean.class)
                    .ifFalseGoto(isFalse)
                    .push(1)
                    .gotoLabel(write)
                    .visitLabel(isFalse)
                    .push(0)
                    .visitLabel(write)
                    .invokeInterface(BlockBuilder.class, "writeByte", BlockBuilder.class, int.class)
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop()

                    .gotoLabel(done)
                    .visitLabel(isNull)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .visitLabel(done);
        }

        @Override
        protected void writeLong(Block body, Variable blockBuilder, Variable arg, int i)
        {
            LabelNode isNull = new LabelNode("isNull" + i);
            LabelNode done = new LabelNode("done" + i);
            body
                    .getVariable(arg)
                    .ifNullGoto(isNull)
                    .getVariable(blockBuilder)

                    .getVariable(arg)
                    .invokeVirtual(Long.class, "longValue", long.class)
                    .invokeInterface(BlockBuilder.class, "writeLong", BlockBuilder.class, long.class)
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop()

                    .gotoLabel(done)
                    .visitLabel(isNull)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .visitLabel(done);
        }

        @Override
        protected void writeDouble(Block body, Variable blockBuilder, Variable arg, int i)
        {
            LabelNode isNull = new LabelNode("isNull" + i);
            LabelNode done = new LabelNode("done" + i);
            body
                    .getVariable(arg)
                    .ifNullGoto(isNull)
                    .getVariable(blockBuilder)

                    .getVariable(arg)
                    .invokeVirtual(Double.class, "doubleValue", double.class)
                    .invokeInterface(BlockBuilder.class, "writeDouble", BlockBuilder.class, double.class)
                    .pop()

                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "closeEntry", BlockBuilder.class)
                    .pop()

                    .gotoLabel(done)
                    .visitLabel(isNull)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop()
                    .visitLabel(done);
        }
    }

    public static Class<?> generateBox(String name, Class<?> valueClass)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(name + "$box"),
                type(Box.class, valueClass));

        MethodDefinition methodDefinition = definition.declareConstructor(a(PUBLIC), ImmutableList.of(arg("value", valueClass)));
        methodDefinition.getBody()
                .getVariable(methodDefinition.getThis())
                .getVariable(methodDefinition.getScope().getVariable("value"))
                .invokeConstructor(Box.class, Object.class)
                .ret();

        return defineClass(definition, Object.class, new CallSiteBinder().getBindings(), new DynamicClassLoader(StructManager.class.getClassLoader()));
    }

    // TODO compile? bench bitch
    // TODO direct in-session serializers via thread local, no intermediate lists / slices
    // raw is trivial just dont add names
    public final Map<String, StructInfo> structInfoMap = new MapMaker().makeMap();

    public static final class StructInfo
    {
        private final RowType rowType;
        private final Class<?> sliceBoxClass;
        private final Class<?> listBoxClass;
        private final StdSerializer serializer;
        private final StdDeserializer deserializer;

        public StructInfo(RowType rowType, Class<?> sliceBoxClass, Class<?> listBoxClass, StdSerializer serializer, StdDeserializer deserializer)
        {
            this.rowType = rowType;
            this.sliceBoxClass = sliceBoxClass;
            this.listBoxClass = listBoxClass;
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public String getName()
        {
            return rowType.getTypeSignature().getBase();
        }
    }

    public static class RowTypeSerializer extends StdSerializer<Box<List>>
    {
        private final RowType rowType;

        public RowTypeSerializer(RowType rowType, Class listBoxClass)
        {
            super(listBoxClass);
            this.rowType = rowType;
        }

        @Override
        public void serialize(Box<List> value, JsonGenerator jgen, SerializerProvider provider) throws IOException
        {
            checkNotNull(value);
            List list = value.getValue();
            if (list == null) {
                jgen.writeNull();
                return;
            }
            List<RowType.RowField> rowFields = rowType.getFields();
            checkArgument(list.size() == rowFields.size());
            jgen.writeStartObject();
            for (int i = 0; i < list.size(); ++i) {
                RowType.RowField rowField = rowFields.get(i);
                // FIXME nameless = lists
                jgen.writeObjectField(rowField.getName().get(), list.get(i));
            }
            jgen.writeEndObject();
        }
    }

    public static class RowTypeDeserializer extends StdDeserializer<Box<Slice>>
    {
        private final RowType rowType;

        public RowTypeDeserializer(RowType rowType, Class sliceBoxClass)
        {
            super(sliceBoxClass);
            this.rowType = rowType;
        }

        @Override
        public Box<Slice> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
        {
            return null;
        }
    }

    public Object boxValue(Type type, Object value, @Nullable ConnectorSession connectorSession)
    {
        String typeName = type.getTypeSignature().getBase();
        if (value == null) {
            return null;
        }
        if (value instanceof Slice) {
            Slice slice = (Slice) value;
            com.facebook.presto.spi.block.Block block = new com.facebook.presto.spi.block.FixedWidthBlockBuilder(slice.length(), new com.facebook.presto.spi.block.BlockBuilderStatus()).writeBytes(slice, 0, slice.length()).closeEntry().build();
            value = type.getObjectValue(connectorSession, block, 0);
        }
        StructInfo structInfo = structInfoMap.get(typeName);
        if (type instanceof RowType && structInfo != null) {
            checkState(value instanceof List);
            List list = (List) value;
            return boxRow((RowType) type, list, connectorSession);
        }
        else if (type instanceof ArrayType) {
            checkState(value instanceof List);
            List list = (List) value;
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getTypeParameters().get(0);
            String elementTypeName = elementType.getTypeSignature().getBase();
            if (elementType instanceof RowType && structInfo != null) {
                return ImmutableList.copyOf(Lists.<List, Box<List>>transform(list, e -> boxRow((RowType) elementType, e, connectorSession)));
            }
            else {
                return value;
            }
        }
        else if (type instanceof MapType) {
            checkState(value instanceof Map);
            Map map = (Map) value;
            MapType mapType = (MapType) type;
            // FIXME keyzzz
            Type valueType = mapType.getTypeParameters().get(1);
            String valueTypeName = valueType.getTypeSignature().getBase();
            if (valueType instanceof RowType && structInfo != null) {
                return ImmutableMap.copyOf(Maps.<Object, List, Box<List>>transformValues(map, e -> boxRow((RowType) valueType, e, connectorSession)));
            }
            else {
                return value;
            }
        }
        else {
            return value;
        }
    }

    public Box<List> boxRow(RowType rowType, List rowValues, @Nullable ConnectorSession connectorSession)
    {
        StructInfo structInfo = structInfoMap.get(rowType.getTypeSignature().getBase());
        Class listBoxClass = structInfo.listBoxClass;
        Constructor<Box<List>> listBoxCtor;
        try {
            listBoxCtor = listBoxClass.getDeclaredConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
        List<Object> boxedValues = newArrayList();
        List<RowType.RowField> rowFields = rowType.getFields();
        for (int i = 0; i < rowFields.size(); ++i) {
            RowType.RowField rowField = rowFields.get(i);
            Type fieldType = rowField.getType();
            Object fieldValue = rowValues.get(i);
            Object boxedValue = boxValue(fieldType, fieldValue, connectorSession);
            boxedValues.add(boxedValue);
        }
        try {
            // FIXME lel
            return listBoxCtor.newInstance(Collections.unmodifiableList(boxedValues));
        }
        catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
            throw Throwables.propagate(e);
        }
    }

    public static class StructDefinition
    {
        public static class Field
        {
            @Nullable
            private final String name;
            private final String type;

            @JsonCreator
            public Field(
                    @JsonProperty("name") @Nullable String name,
                    @JsonProperty("type") String type)
            {
                this.name = name;
                this.type = type;
            }

            @Nullable
            public String getName()
            {
                return name;
            }

            public String getType()
            {
                return type;
            }
        }

        private final String name;
        private final List<Field> fields;

        @JsonCreator
        public StructDefinition(
                @JsonProperty("name") String name,
                @JsonProperty("fields") List<Field> fields)
        {
            this.name = name;
            this.fields = fields;
        }

        public String getName()
        {
            return name;
        }

        public List<Field> getFields()
        {
            return fields;
        }
    }

    public RowType buildRowType(StructDefinition def)
    {
        List<String> fieldNames = def.getFields().stream().map(StructDefinition.Field::getName).filter(Objects::nonNull).collect(toImmutableList());
        checkArgument(fieldNames.isEmpty() || fieldNames.size() == def.getFields().size());
        return new RowType(
                parameterizedTypeName(def.getName()),
                def.getFields().stream().map(f -> typeRegistry.getType(parseTypeSignature(f.getType()))).collect(toImmutableList()),
                fieldNames.isEmpty() ? Optional.empty() : Optional.of(fieldNames)
        );
    }

    public void registerStruct(RowType rowType)
    {
        String name = rowType.getTypeSignature().getBase();
        typeRegistry.addType(rowType);
        metadata.addFunctions(
                new FunctionListBuilder(typeRegistry)
                        .scalar(new RowTypeConstructorCompiler().run(rowType, name + "_strict"))
                        .scalar(new NullableRowTypeConstructorCompiler().run(rowType, name))
                        .getFunctions());
        Class<?> sliceBoxClass = generateBox(name, Slice.class);
        Class<?> listBoxClass = generateBox(name, List.class);
        StdSerializer serializer = new RowTypeSerializer(rowType, listBoxClass);
        StdDeserializer deserializer = new RowTypeDeserializer(rowType, sliceBoxClass);

        SimpleModule module = new SimpleModule();
        module.addSerializer(listBoxClass, serializer);
        module.addDeserializer(listBoxClass, deserializer);
        OBJECT_MAPPER.get().registerModule(module);

        StructInfo structInfo = new StructInfo(
                rowType,
                sliceBoxClass,
                listBoxClass,
                serializer,
                deserializer
        );
        structInfoMap.put(name, structInfo);

        // TIE THE KNOT - BOX RECURSION
    }
}
