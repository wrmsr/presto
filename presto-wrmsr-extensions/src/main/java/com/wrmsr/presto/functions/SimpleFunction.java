package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.*;
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
import static com.wrmsr.presto.util.Lists.listOf;

public abstract class SimpleFunction
    extends ParametricScalar
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
        signature = new Signature(functionName, typeParameters, functionReturnType, argumentTypes, true, false);

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
