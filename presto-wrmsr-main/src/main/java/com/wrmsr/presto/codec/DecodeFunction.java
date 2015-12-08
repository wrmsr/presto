package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.function.FunctionRegistration;

import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;

public class DecodeFunction
        extends SqlScalarFunction
        implements FunctionRegistration.Self
{
    public static final String NAME = "decode";

    public DecodeFunction()
    {
        super(NAME, ImmutableList.of(typeParameter("F"), typeParameter("T")), "F", ImmutableList.of("T"));
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
        return true;
    }

    @Override
    public String getDescription()
    {
        return "decode";
    }
}
