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

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.collect.Lists.listOf;

public abstract class StringVarargsFunction
        extends SqlScalarFunction
{
    private final String functionName;
    private final String description;
    private final List<String> fixedParameterTypes;
    private final int varargGroupSize;
    private final String functionReturnType;
    private final String methodName;
    private final List<Class<?>> fixedMethodParametersClasses;

    private final Signature signature;
    private final MethodHandle methodHandle;

    public StringVarargsFunction(
            String functionName,
            String description,
            List<String> fixedParameterTypes,
            int varargGroupSize,
            String functionReturnType,
            String methodName,
            List<Class<?>> fixedMethodParametersClasses)
    {
        super(functionName, buildTypeParameters(fixedParameterTypes), functionReturnType, buildArgumentTypes(fixedParameterTypes), true);
        this.functionName = functionName;
        this.description = description;
        this.fixedParameterTypes = fixedParameterTypes;
        this.varargGroupSize = varargGroupSize;
        this.functionReturnType = functionReturnType;
        this.methodName = methodName;
        this.fixedMethodParametersClasses = fixedMethodParametersClasses;

        List<TypeParameter> typeParameters = buildTypeParameters(fixedParameterTypes);
        List<String> argumentTypes = buildArgumentTypes(fixedParameterTypes);
        signature = new Signature(functionName, FunctionKind.SCALAR, typeParameters, functionReturnType, argumentTypes, true);

        List<Class<?>> parameterTypes = newArrayList();
        for (Class<?> c : fixedMethodParametersClasses) {
            parameterTypes.add(c);
        }
        parameterTypes.add(Object[].class);
        methodHandle = Reflection.methodHandle(getClass(), methodName, (Class<?>[]) parameterTypes.toArray(new Class<?>[parameterTypes.size()]));
    }

    protected static List<TypeParameter> buildTypeParameters(List<String> fixedParameterTypes)
    {
        List<TypeParameter> typeParameters = newArrayList();
        for (String s : fixedParameterTypes) {
            typeParameters.add(comparableTypeParameter(s));
        }
        typeParameters.add(typeParameter("E"));
        return typeParameters;
    }

    protected static List<String> buildArgumentTypes(List<String> fixedParameterTypes)
    {
        List<String> argumentTypes = newArrayList();
        for (String s : fixedParameterTypes) {
            argumentTypes.add(s);
        }
        argumentTypes.add("E");
        return argumentTypes;
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
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");
        checkArgument(type.getJavaType() == Slice.class);
        checkArgument(arity % varargGroupSize == fixedParameterTypes.size());
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(Slice.class);
        for (int i = 1; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        List<TypeSignature> argumentTypes = newArrayList();
        for (String s : fixedParameterTypes) {
            argumentTypes.add(parseTypeSignature(s));
        }
        argumentTypes.addAll(listOf(arity - fixedParameterTypes.size(), type.getTypeSignature()));

        MethodHandle methodHandle = bindMethodHandle().asVarargsCollector(Object[].class);
        return new ScalarFunctionImplementation(false, listOf(arity, false), methodHandle, isDeterministic());

        /*
        return new FunctionInfo(
                new Signature(
                        functionName,
                        FunctionKind.SCALAR,
                        parseTypeSignature(functionReturnType),
                        argumentTypes,
                        true),
                getDescription(),
                isHidden(),
                methodHandle,
                isDeterministic(),
                true,
                listOf(arity, true));
        */
    }
}
