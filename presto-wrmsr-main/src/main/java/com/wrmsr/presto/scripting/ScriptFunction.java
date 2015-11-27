package com.wrmsr.presto.scripting;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

// scripting:string -> function:string -> arg:object...
public class ScriptFunction
    extends SqlScalarFunction
{
    public static final String NAME = "script";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ScriptFunction.class, "script", Context.class, ConnectorSession.class, Object[].class);

    private final ScriptingManager scriptingManager;
    private final int arity;

    public ScriptFunction(ScriptingManager scriptingManager, int arity) // bahahaha
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

        this.arity = arity;
        this.scriptingManager = scriptingManager;
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
        private final ScriptingManager scriptingManager;
        private final Type retType;
        private final List<Type> argTypes;

        public Context(ScriptingManager scriptingManager, Type retType, List<Type> argTypes)
        {
            this.scriptingManager = scriptingManager;
            this.retType = retType;
            this.argTypes = argTypes;
        }
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == this.arity);
        Type retType = types.get("R");
        List<Type> argTypes = IntStream.range(0, arity).boxed().map(n -> types.get("T" + n.toString())).collect(toImmutableList());
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new Context(scriptingManager, retType, argTypes));
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    private static Slice script(Context context, ConnectorSession session, Slice scriptingName, Slice functionName, Object... args)
    {
        Scripting scripting = context.scriptingManager.getScripting(scriptingName.toStringUtf8());
        scripting.invokeFunction(functionName.toStringUtf8(), args);
        throw new IllegalStateException();
    }
}
