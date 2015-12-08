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
package com.wrmsr.presto.serialization;

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.struct.StructManager;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

// TODO: COMPILE.
// FIXME LOLOL RECURSION LINKED LISTS
public class SerializeFunction
        extends SqlScalarFunction
{
    private static final String FUNCTION_NAME = "serialize";
    private static final MethodHandle METHOD_HANDLE = methodHandle(SerializeFunction.class, "serialize", SerializationContext.class, ConnectorSession.class, Object.class);

    private static class SerializationContext
    {
        private final StructManager structManager;
        private final Supplier<ObjectMapper> objectMapperSupplier;
        private final Type type;

        public SerializationContext(StructManager structManager, Supplier<ObjectMapper> objectMapperSupplier, Type type)
        {
            this.structManager = structManager;
            this.objectMapperSupplier = objectMapperSupplier;
            this.type = type;
        }
    }

    private final StructManager structManager;

    @Inject
    public SerializeFunction(StructManager structManager)
    {
        super(FUNCTION_NAME, ImmutableList.of(typeParameter("E")), StandardTypes.VARBINARY, ImmutableList.of("E"));
        this.structManager = structManager;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Returns the cardinality (length) of the array";
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1);
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new SerializationContext(structManager, OBJECT_MAPPER, type));

        /*
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            for (RowType.RowField field : rowType.getFields()) {
                FunctionInfo functionInfo = functionRegistry.resolveFunction(new QualifiedName("serialize"), ImmutableList.of(field.getType().getTypeSignature()), false);
            }
        }
        */

        // functionRegistry.resolveFunction(new QualifiedName("serialize"), List< TypeSignature > parameterTypes, false)

        // Signature sig = new Signature(FUNCTION_NAME, FunctionKind.SCALAR, parseTypeSignature(StandardTypes.VARBINARY), type.getTypeSignature());
        // return new ScalarFunctionImplementation(sig, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(true));
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    public static Slice serialize(SerializationContext serializationContext, ConnectorSession session, @Nullable Object object)
    {
        try {
            Object boxed = serializationContext.structManager.boxValue(serializationContext.type, object, session);
            return Slices.wrappedBuffer(serializationContext.objectMapperSupplier.get().writeValueAsBytes(boxed));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
