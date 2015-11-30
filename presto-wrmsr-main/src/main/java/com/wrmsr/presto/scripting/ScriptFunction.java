package com.wrmsr.presto.scripting;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
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
import static com.wrmsr.presto.util.collect.Lists.listOf;

// scripting:string -> function:string -> arg:object...
public class ScriptFunction
    extends SqlScalarFunction
{
    public static final String NAME = "script";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ScriptFunction.class, "script", Context.class, ConnectorSession.class, Slice.class, Slice.class, Object[].class);

    private final ScriptingManager scriptingManager;
    private final Type retType;
    private final int arity;

    public ScriptFunction(ScriptingManager scriptingManager, Type retType, int arity) // bahahaha
    {
        super(
                NAME,
                IntStream.range(0, arity).boxed().map(n -> new TypeParameter("T" + n.toString(), false, false, null)).collect(toImmutableList()),
                retType.getTypeSignature().getBase(),
                ImmutableList.<String>builder()
                        .add("varchar")
                        .add("varchar")
                        .addAll(IntStream.range(0, arity).boxed().map(n -> "T" + n.toString()).collect(toImmutableList()))
                        .build());

        this.arity = arity;
        this.retType = retType;
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
        checkArgument(arity == this.arity + 2);
        List<Type> argTypes = ImmutableList.<Type>builder()
                .add(VarcharType.VARCHAR)
                .add(VarcharType.VARCHAR)
                .addAll(IntStream.range(0, arity - 2).boxed().map(n -> types.get("T" + n.toString())).collect(toImmutableList()))
                .build();
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new Context(scriptingManager, retType, argTypes)).asVarargsCollector(Object[].class);
        // FIXME nullable controls
        return new ScalarFunctionImplementation(true, listOf(arity, true), methodHandle, isDeterministic());
    }

    public static Object script(Context context, ConnectorSession session, Slice scriptingName, Slice functionName, Object... args)
    {
        Scripting scripting = context.scriptingManager.getScripting(scriptingName.toStringUtf8());
        scripting.invokeFunction(functionName.toStringUtf8(), args);
        throw new IllegalStateException();
    }
}
