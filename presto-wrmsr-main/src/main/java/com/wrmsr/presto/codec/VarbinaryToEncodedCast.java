package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class VarbinaryToEncodedCast
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(VarbinaryToEncodedCast.class, "castSlice", Slice.class);

    private final TypeCodec typeCodec;

    public VarbinaryToEncodedCast(TypeCodec typeCodec)
    {
        super(CAST, ImmutableList.of(typeParameter("T")), typeCodec.getName() + "<T>", ImmutableList.of("varbinary"));
        this.typeCodec = typeCodec;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), METHOD_HANDLE, true);
    }

    public static Slice castSlice(Slice slice)
    {
        return slice;
    }
}
