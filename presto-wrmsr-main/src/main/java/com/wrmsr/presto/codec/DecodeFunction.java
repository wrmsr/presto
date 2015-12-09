package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class DecodeFunction
        extends SqlScalarFunction
{
    public static final String NAME = "decode";
    private static final MethodHandle METHOD_HANDLE = methodHandle(DecodeFunction.class, "decodeSlice", Codec.class, Slice.class);

    private final TypeCodec typeCodec;

    public DecodeFunction(TypeCodec typeCodec)
    {
        super(NAME, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of(typeCodec.getName() + "<T>"));
        this.typeCodec = typeCodec;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1);
        Type fromType = Iterables.getOnlyElement(types.values());
        Codec<Slice, Slice> codec = typeCodec.getCodec(fromType);
        MethodHandle mh = METHOD_HANDLE.bindTo(codec);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), mh, true);
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
        return "decode";
    }

    public static Slice decodeSlice(Codec<Slice, Slice> codec, Slice slice)
    {
        return codec.decode(slice);
    }
}
