package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.*;
import com.facebook.presto.spi.type.StandardTypes;
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

public abstract class StringVarargsFunction
        extends ParametricScalar
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
        this.functionName = functionName;
        this.description = description;
        this.fixedParameterTypes = fixedParameterTypes;
        this.varargGroupSize = varargGroupSize;
        this.functionReturnType = functionReturnType;
        this.methodName = methodName;
        this.fixedMethodParametersClasses = fixedMethodParametersClasses;

        List<TypeParameter> typeParameters = newArrayList();
        List<String> argumentTypes = newArrayList();
        for (String s : fixedParameterTypes) {
            typeParameters.add(comparableTypeParameter(s));
            argumentTypes.add(s);
        }
        typeParameters.add(typeParameter("E"));
        argumentTypes.add("E");
        signature = new Signature(functionName, typeParameters, functionReturnType, argumentTypes, true, false);

        List<Class<?>> parameterTypes = newArrayList();
        for (Class<?> c : fixedMethodParametersClasses) {
            parameterTypes.add(c);
        }
        parameterTypes.add(Object[].class);
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
    }
}
