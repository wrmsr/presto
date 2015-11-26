package com.wrmsr.presto.scripting;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

// scripting:string -> function:string -> arg:object...
public class ScriptFunction
    extends SqlScalarFunction
{
    public static final String NAME = "script";

    public ScriptFunction(int arity) // bahahaha
    {
        super(
                NAME,
                ImmutableList.<TypeParameter>builder()
                        .add(new TypeParameter("R", false, false, null))
                        .addAll(IntStream.range(0, arity).boxed().map(n -> new TypeParameter("T" + n.toString(), false, false, null)).collect(toImmutableList()))
                        .build(),
                "R",
                ImmutableList.<String>builder()
                        .add("varchar")
                        .add("varchar")
                        .addAll(IntStream.range(0, arity).boxed().map(n -> "T" + n.toString()).collect(toImmutableList()))
                        .build());
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
        return "invoke script";
    }

    private static class Context
    {
        private final String scripting;
        private final String name;

        public Context(String scripting, String name)
        {
            this.scripting = scripting;
            this.name = name;
        }
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return null;
    }

    public static Slice script(Context context, Object... args)
    {

    }
}
