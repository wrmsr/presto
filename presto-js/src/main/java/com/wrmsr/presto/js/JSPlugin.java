package com.wrmsr.presto.js;

// import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
// import com.facebook.presto.type.ParametricType;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class JSPlugin
        implements Plugin
{
    private TypeManager typeManager;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
    }

    @Inject
    public void setTypeManager(TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
//        if (type == FunctionFactory.class) {
//            return ImmutableList.of(type.cast(new MLFunctionFactory(typeManager)));
//        }
//        else if (type == Type.class) {
//            return ImmutableList.of(type.cast(MODEL), type.cast(REGRESSOR));
//        }
//        else if (type == ParametricType.class) {
//            return ImmutableList.of(type.cast(new ClassifierParametricType()));
//        }
        return ImmutableList.of();
    }
}
