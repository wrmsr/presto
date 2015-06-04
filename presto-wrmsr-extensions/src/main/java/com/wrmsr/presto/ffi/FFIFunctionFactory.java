package com.wrmsr.presto.ffi;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

public class FFIFunctionFactory
        implements FunctionFactory
{
    private final TypeManager typeManager;

    public FFIFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .scalar(CompressionFunctions.class)
                .function(SerializeFunction.SERIALIZE)
                .getFunctions();
    }
}

