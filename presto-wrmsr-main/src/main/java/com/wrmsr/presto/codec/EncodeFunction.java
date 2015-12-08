package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.FunctionResolver;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.function.FunctionRegistration;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;

public class EncodeFunction
        extends SqlScalarFunction
{
    private final TypeCodec typeCodec;

    @Inject
    public EncodeFunction(TypeCodec typeCodec)
    {
        super(typeCodec.getName(), ImmutableList.of(typeParameter("T")), typeCodec.getName() + "<T>", ImmutableList.of("T"));
        this.typeCodec = typeCodec;
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
        return "encode " + typeCodec.getName();
    }
}
