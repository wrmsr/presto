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

import com.facebook.presto.metadata.*;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ConnectFunction
    extends ParametricScalar
{
    /*
    private static final Signature SIGNATURE = new Signature(
            "connect", ImmutableList.of(comparableTypeParameter("varchar"), comparableTypeParameter("varchar")), StandardTypes.VARBINARY, ImmutableList.of("E"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(SerializeFunction.class, "serialize", SerializationContext.class, ConnectorSession.class, Object.class);

    public static Slice serialize(SerializationContext serializationContext, ConnectorSession session, @Nullable Object object)
    {

    }
    */

    @Override
    public Signature getSignature()
    {
        return null;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return null;
    }
}
