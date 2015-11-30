package com.wrmsr.presto.scripting;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.functions.StructManager;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.ClassUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
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

        String name = "script_invoker";

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(name),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parametersBuilder = ImmutableList.builder();
        parametersBuilder.add(arg("context", Context.class));
        parametersBuilder.add(arg("session", ConnectorSession.class));
        parametersBuilder.add(arg("scriptingName", Slice.class));
        parametersBuilder.add(arg("functionName", Slice.class));
        for (int i = 0; i < arity - 2; i++) {
            Type argType = types.get("T" + i);
            Class<?> javaType = argType.getJavaType();
            // javaType = ClassUtils.primitiveToWrapper(javaType);
            parametersBuilder.add(arg("arg" + i, javaType));
        }
        List<Parameter> parameters = parametersBuilder.build();

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(com.facebook.presto.spi.block.Block.class), parameters);
        methodDefinition.declareAnnotation(ScalarFunction.class);
        methodDefinition.declareAnnotation(SqlType.class).setValue("value", retType.getTypeSignature().toString());
        methodDefinition.declareParameterAnnotation(SqlType.class, 2).setValue("value", "varchar");
        methodDefinition.declareParameterAnnotation(SqlType.class, 3).setValue("value", "varchar");
        for (int i = 0; i < arity - 2; i++) {
            methodDefinition.declareParameterAnnotation(SqlType.class, i + 4).setValue("value", types.get("T" + i).getTypeSignature().toString());
        }

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        ByteCodeBlock body = methodDefinition.getBody();

        body
                .getVariable(scope.getVariable("context"))
                .getVariable(scope.getVariable("session"))
                .getVariable(scope.getVariable("scriptingName"))
                .getVariable(scope.getVariable("functionName"))
                .push(arity - 2)
                .newArray(Object.class);

        for (int i = 0; i < arity - 2; i++) {
            Variable arg = scope.getVariable("arg" + i);
            body
                    .dup()
                    .push(i)
                    .getVariable(arg);

            Type argType = types.get("T" + i);
            Class<?> javaType = argType.getJavaType();

            if (javaType == boolean.class) {
                body.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
            }
            else if (javaType == long.class) {
                body.invokeStatic(Long.class, "valueOf", Long.class, long.class);
            }
            else if (javaType == double.class) {
                body.invokeStatic(Double.class, "valueOf", Double.class, double.class);
            }

            body.putObjectArrayElement();
        }

        body
                .invokeStatic(ScriptFunction.class, "script", Context.class, ConnectorSession.class, Slice.class, Slice.class, Object[].class)
                .retObject();

        Class<?> cls = defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(StructManager.RowTypeConstructorCompiler.class.getClassLoader()));

        /*
        List<Type> argTypes = ImmutableList.<Type>builder()
                .add(VarcharType.VARCHAR)
                .add(VarcharType.VARCHAR)
                .addAll(IntStream.range(0, arity - 2).boxed().map(n -> types.get("T" + n.toString())).collect(toImmutableList()))
                .build();

        MethodHandle methodHandle = METHOD_HANDLE.bindTo(new Context(scriptingManager, retType, argTypes)).asVarargsCollector(Object[].class);
        // FIXME nullable controls
        return new ScalarFunctionImplementation(true, listOf(arity, true), methodHandle, isDeterministic());
        */

        throw new IllegalStateException();
    }

    public static Object script(Context context, ConnectorSession session, Slice scriptingName, Slice functionName, Object... args)
    {
        Scripting scripting = context.scriptingManager.getScripting(scriptingName.toStringUtf8());
        scripting.invokeFunction(functionName.toStringUtf8(), args);
        throw new IllegalStateException();
    }
}
