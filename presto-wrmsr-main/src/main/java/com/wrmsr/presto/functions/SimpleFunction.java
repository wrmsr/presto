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
import com.facebook.presto.metadata.FunctionType;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.google.common.collect.Lists.newArrayList;

public abstract class SimpleFunction
        implements ParametricFunction
{
    private final String functionName;
    private final String description;
    private final List<String> parameterTypes;
    private final String functionReturnType;
    private final String methodName;
    private final List<Class<?>> methodParametersClasses;

    private final Signature signature;
    private final MethodHandle methodHandle;

    public SimpleFunction(
            String functionName,
            String description,
            List<String> parameterTypes,
            String functionReturnType,
            String methodName,
            List<Class<?>> methodParametersClasses)
    {
        this.functionName = functionName;
        this.description = description;
        this.parameterTypes = parameterTypes;
        this.functionReturnType = functionReturnType;
        this.methodName = methodName;
        this.methodParametersClasses = methodParametersClasses;

        List<TypeParameter> typeParameters = newArrayList();
        List<String> argumentTypes = newArrayList();
        for (String s : parameterTypes) {
            typeParameters.add(comparableTypeParameter(s));
            argumentTypes.add(s);
        }
        signature = new Signature(functionName, FunctionType.SCALAR, typeParameters, functionReturnType, argumentTypes, false);

        methodHandle = Reflection.methodHandle(getClass(), methodName, (Class<?>[]) parameterTypes.toArray(new Class<?>[parameterTypes.size()]));
    }

    @Override
    public Signature getSignature()
    {
        return signature;
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
        return description;
    }

    protected MethodHandle bindMethodHandle()
    {
        return methodHandle;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        /*
        checkArgument(type.getJavaType() == Slice.class);
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(Slice.class);
        for (int i = 1; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        List<TypeSignature> argumentTypes = newArrayList();
        for (String s : fixedParameterTypes) {
            argumentTypes.add(parseTypeSignature(s));
        }
        argumentTypes.addAll(listOf(arity - parameterTypes.size(), type.getTypeSignature()));

        MethodHandle methodHandle = bindMethodHandle().asVarargsCollector(Object[].class);
        return new FunctionInfo(
                new Signature(
                        functionName,
                        parseTypeSignature(functionReturnType),
                        argumentTypes),
                getDescription(),
                isHidden(),
                methodHandle,
                isDeterministic(),
                true,
                listOf(arity, true));
        */
        return null;
    }
}
