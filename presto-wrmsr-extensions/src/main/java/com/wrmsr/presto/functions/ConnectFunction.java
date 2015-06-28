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
