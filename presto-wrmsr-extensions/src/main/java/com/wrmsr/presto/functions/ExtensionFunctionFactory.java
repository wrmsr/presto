package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.wrmsr.presto.functions.CompressionFunctions;
import com.wrmsr.presto.functions.Hash;
import com.wrmsr.presto.functions.SerializeFunction;

import java.util.List;

public class ExtensionFunctionFactory
        implements com.facebook.presto.metadata.FunctionFactory
{
    private final TypeManager typeManager;

    public ExtensionFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .scalar(CompressionFunctions.class)
                .function(SerializeFunction.SERIALIZE)
                .function(Hash.HASH)
                .getFunctions();
    }
}

