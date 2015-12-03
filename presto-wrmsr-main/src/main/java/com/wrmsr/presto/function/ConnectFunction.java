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
package com.wrmsr.presto.function;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;
import java.util.Map;

// String name, Properties properties
public class ConnectFunction
        extends SqlScalarFunction
{
    /*
    private static final Signature SIGNATURE = new Signature(
            "connect", ImmutableList.of(comparableTypeParameter("varchar"), comparableTypeParameter("varchar")), StandardTypes.VARBINARY, ImmutableList.of("E"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(SerializeFunction.class, "serialize", SerializationContext.class, ConnectorSession.class, Object.class);

    public static Slice serialize(SerializationContext serializationContext, ConnectorSession session, @Nullable Object object)
    {

    }
    */


    /* @Override
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
    */

    public ConnectFunction(String name, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes)
    {
        super(name, typeParameters, returnType, argumentTypes);
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
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
}
