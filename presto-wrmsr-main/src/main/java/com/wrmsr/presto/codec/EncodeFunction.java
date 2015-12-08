package com.wrmsr.presto.codec;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SignatureBinder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.function.SignatureBinderRegistration;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;

public class EncodeFunction
        extends SqlScalarFunction
        implements FunctionRegistration.Self, SignatureBinder, SignatureBinderRegistration.Self
{
    public static final String NAME = "encode";

    private final TypeCodecManager typeCodecManager;

    @Inject
    public EncodeFunction(TypeCodecManager typeCodecManager)
    {
        super(NAME, ImmutableList.of(typeParameter("F"), typeParameter("T")), "T", ImmutableList.of("varchar", "F"));
        this.typeCodecManager = typeCodecManager;
    }

    @Nullable
    @Override
    public Signature bindSignature(Signature signature, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        if (NAME.equals(signature.getName())) {
        }
        return null;
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
//        typeCodecManager.getTypeCodec()
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
        return "encode";
    }
}
