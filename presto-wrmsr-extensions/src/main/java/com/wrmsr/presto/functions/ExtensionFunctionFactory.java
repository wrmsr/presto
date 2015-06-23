package com.wrmsr.presto.functions;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;

public class ExtensionFunctionFactory
        implements com.facebook.presto.metadata.FunctionFactory
{
    private final TypeManager typeManager;
    private final FunctionRegistry functionRegistry;

    public ExtensionFunctionFactory(TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        this.typeManager = typeManager;
        this.functionRegistry = functionRegistry;
    }

    @Override
    public List<ParametricFunction> listFunctions()
    {
        return new FunctionListBuilder(typeManager)
                .scalar(CompressionFunctions.class)
                .function(new SerializeFunction(functionRegistry))
                .function(Hash.HASH)
                .getFunctions();
    }
}

