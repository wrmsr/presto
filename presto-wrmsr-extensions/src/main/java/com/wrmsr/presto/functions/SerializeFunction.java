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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

// TODO: COMPILE.
// FIXME LOLOL RECURSION LINKED LISTS
public class SerializeFunction
    extends ParametricScalar
{
    private static final Signature SIGNATURE = new Signature( // FIXME nullable
            "serialize", ImmutableList.of(typeParameter("E")), StandardTypes.VARBINARY, ImmutableList.of("E"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(SerializeFunction.class, "serialize", Type.class, ConnectorSession.class, Object.class);

    private final FunctionRegistry functionRegistry;

    private final ThreadLocal<ConnectorSession> connectorSessionThreadLocal = new ThreadLocal<ConnectorSession>();

    public SerializeFunction(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = functionRegistry;
    }

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
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
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Cardinality expects only one argument");
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);

        /*
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            for (RowType.RowField field : rowType.getFields()) {
                FunctionInfo functionInfo = functionRegistry.resolveFunction(new QualifiedName("serialize"), ImmutableList.of(field.getType().getTypeSignature()), false);
            }
        }
        */

        // functionRegistry.resolveFunction(new QualifiedName("serialize"), List< TypeSignature > parameterTypes, false)
        return new FunctionInfo(new Signature("serialize", parseTypeSignature(StandardTypes.VARBINARY), type.getTypeSignature()), getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(true));
    }

    public static Slice serialize(Type type, ConnectorSession session, @Nullable Object object)
    {
        try {
            return Slices.wrappedBuffer(OBJECT_MAPPER.get().writeValueAsBytes(object));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
