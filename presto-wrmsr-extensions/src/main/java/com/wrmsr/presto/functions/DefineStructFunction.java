package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableList;

public class DefineStructFunction
            extends ParametricScalar
{
    private static final Signature SIGNATURE = new Signature("define_struct", ImmutableList.of(comparableTypeParameter("varchar"), typeParameter("E")), "varchar", ImmutableList.of("varchar", "E"), true, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(DefineStructFunction.class, "define_struct", DefineStructContext.class, Slice.class, Slice.class, Slice[].class);

    private final TypeRegistrar typeRegistrar;

    public DefineStructFunction(TypeRegistrar typeRegistrar)
    {
        this.typeRegistrar = typeRegistrar;
    }

    private static class DefineStructContext
    {
        private final TypeRegistrar typeRegistrar;

        public DefineStructContext(TypeRegistrar typeRegistrar)
        {
            this.typeRegistrar = typeRegistrar;
        }
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
        return "define a new struct type";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("E");

        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        builder.add(Slice.class);
        for (int i = 1; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new DefineStructContext(typeRegistrar));
        return new FunctionInfo(new Signature("define_struct", parseTypeSignature(StandardTypes.VARCHAR), IntStream.range(0, arity).boxed().map(i -> type.getTypeSignature()).collect(toImmutableList())), getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(true));
    }

    public static Slice defineStruct(DefineStructContext context, Slice name, Slice... strs)
    {
        return null;
    }
}
