package com.wrmsr.presto.ffi;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class SerializeFunction
    extends ParametricScalar
{
    public static final SerializeFunction SERIALIZE = new SerializeFunction();
    private static final Signature SIGNATURE = new Signature(
            "serialize", ImmutableList.of(typeParameter("E")), StandardTypes.VARBINARY, ImmutableList.of("E", StandardTypes.VARCHAR), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(SerializeFunction.class, "serialize", Object.class, String.class);

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
        return new FunctionInfo(new Signature("serialize", parseTypeSignature(StandardTypes.VARBINARY), type.getTypeSignature(), parseTypeSignature(StandardTypes.VARCHAR)), getDescription(), isHidden(), METHOD_HANDLE, isDeterministic(), false, ImmutableList.of(false));
    }

    public static Slice serialize(Object object, String codec)
    {
        return Slices.wrappedBuffer(new byte[]{1, 2, 3});
    }
}
