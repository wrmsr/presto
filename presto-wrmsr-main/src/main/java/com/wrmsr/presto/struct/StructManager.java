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
package com.wrmsr.presto.struct;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.FixedWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.wrmsr.presto.type.WrapperType;
import com.wrmsr.presto.util.Box;
import com.wrmsr.presto.util.CodeGeneration;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class StructManager
{
    private final TypeRegistry typeRegistry;
    private final Metadata metadata;
    private final BlockEncodingSerde blockEncodingSerde;

    @Inject
    public StructManager(TypeRegistry typeRegistry, Metadata metadata, BlockEncodingSerde blockEncodingSerde)
    {
        this.typeRegistry = typeRegistry;
        this.metadata = metadata;
        this.blockEncodingSerde = blockEncodingSerde;
    }

    // TODO compile? bench bitch
    // TODO direct in-session serializers via thread local, no intermediate lists / slices
    // raw is trivial just dont add names
    public final Map<String, StructInfo> structInfoMap = new MapMaker().makeMap();

    public Object boxValue(Type type, Object value, @Nullable ConnectorSession connectorSession)
    {
        type = WrapperType.stripAnnotations(type);
        String typeName = type.getTypeSignature().getBase();
        if (value == null) {
            return null;
        }
        if (value instanceof Slice) {
            Slice slice = (Slice) value;
            Block block = new FixedWidthBlockBuilder(8, slice.length()).writeBytes(slice, 0, slice.length()).closeEntry().build();
            value = type.getObjectValue(connectorSession, block, 0);
        }
        StructInfo structInfo = structInfoMap.get(typeName);
        if (type instanceof RowType && structInfo != null) {
            checkState(value instanceof Block);
            return boxRow((RowType) type, (Block) value, connectorSession);
        }
        else if (type instanceof ArrayType) {
            checkState(value instanceof ArrayBlock);
            ArrayBlock arrayBlock = (ArrayBlock) value;
            ArrayType arrayType = (ArrayType) type;
            List list = newArrayList();
            for (int i = 0; i < arrayBlock.getPositionCount(); ++i) {
                Block item = arrayBlock.getObject(i, Block.class);
                Object itemObject = boxValue(arrayType.getElementType(), item, connectorSession);
                list.add(itemObject);
            }
//            Type elementType = arrayType.getTypeParameters().get(0);
//            if (elementType instanceof RowType && structInfo != null) {
//                return ImmutableList.copyOf(Lists.<List, Box<List>>transform(list, e -> boxRow((RowType) elementType, e, connectorSession)));
//            }
//            else {
//                return value;
//            }
            return list;
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
        else if (value instanceof Block) {
            return type.getObjectValue(connectorSession, (Block) value, 0);
        }
        else {
            return value;
        }
    }

    public Box<List> boxRow(RowType rowType, Block block, @Nullable ConnectorSession connectorSession)
    {
        StructInfo structInfo = structInfoMap.get(rowType.getTypeSignature().getBase());
        Class listBoxClass = structInfo.getListBoxClass();
        Constructor<Box<List>> listBoxCtor;
        try {
            listBoxCtor = listBoxClass.getDeclaredConstructor(List.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

        List<Object> values = new ArrayList<>(block.getPositionCount());

        for (int i = 0; i < block.getPositionCount(); i++) {
            Type fieldType = rowType.getFields().get(i).getType();
            Object fieldValue = fieldType.getObjectValue(connectorSession, block, i);
            values.add(boxValue(fieldType, fieldValue, connectorSession));
        }

        try {
            // FIXME lel
            return listBoxCtor.newInstance(Collections.unmodifiableList(values));
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    public Box<List> boxRow(RowType rowType, List rowValues, @Nullable ConnectorSession connectorSession)
    {
        StructInfo structInfo = structInfoMap.get(rowType.getTypeSignature().getBase());
        Class listBoxClass = structInfo.getListBoxClass();
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
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
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
        typeRegistry.addParametricType(rowType.getParametricType());
        registerStructFunctions(rowType, name);
    }

    public void registerStructFunctions(RowType rowType, String name)
    {
        metadata.addFunctions(
                new FunctionListBuilder()
                        .scalar(new RowTypeConstructorCompiler().run(rowType, name + "_strict"))
                        .scalar(new NullableRowTypeConstructorCompiler().run(rowType, name))
                        .getFunctions());
        Class<?> sliceBoxClass = CodeGeneration.generateBox(name, Slice.class);
        Class<?> listBoxClass = CodeGeneration.generateBox(name, List.class);
        StdSerializer serializer = new ListRowTypeSerializer(rowType, listBoxClass);
        StdDeserializer deserializer = new RowTypeDeserializer(rowType, sliceBoxClass);

        SimpleModule module = new SimpleModule();
        module.addSerializer(listBoxClass, serializer);
        module.addDeserializer(listBoxClass, deserializer);
        OBJECT_MAPPER.get().registerModule(module);

        StructInfo structInfo = new StructInfo(
                rowType,
                listBoxClass,
                serializer,
                deserializer);
        structInfoMap.put(name, structInfo);

        // TIE THE KNOT - BOX RECURSION
    }
}
