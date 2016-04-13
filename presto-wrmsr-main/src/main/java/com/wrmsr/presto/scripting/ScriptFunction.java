/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.scripting;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.util.Box;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.Lists.listOf;
import static java.lang.invoke.MethodHandles.lookup;

// scripting:string -> function:string -> arg:object...
public class ScriptFunction
        extends SqlScalarFunction
{
    private final ScriptingManager scriptingManager;

    public enum ExecutionType
    {
        INVOKE,
        EVAL
    }

    public static final class Config
    {
        private final String name;
        private final Type returnType;
        private final int arity;
        private final ExecutionType executionType;

        public Config(String name, Type returnType, int arity, ExecutionType executionType)
        {
            this.name = name;
            this.returnType = returnType;
            this.arity = arity;
            this.executionType = executionType;
        }
    }

    private final Config config;

    @FunctionalInterface
    public interface Factory
    {
        ScriptFunction create(Config config);
    }

    public static class Registration
            implements FunctionRegistration
    {
        public static class Configs
                extends Box<List<Config>>
        {
            public Configs(List<Config> value)
            {
                super(value);
            }
        }

        private final List<SqlFunction> functions;

        @Inject
        public Registration(Factory factory, Configs configs)
        {
            ImmutableList.Builder<SqlFunction> builder = ImmutableList.builder();
            for (Config config : configs.getValue()) {
                builder.add(factory.create(config));
            }
            functions = builder.build();
        }

        @Override
        public List<SqlFunction> getFunctions(TypeManager typeManager)
        {
            return functions;
        }
    }

    @Inject
    public ScriptFunction(ScriptingManager scriptingManager, @Assisted Config config)
    {
        super(
                config.name,
                IntStream.range(0, config.arity).boxed().map(n -> new TypeVariableConstraint("T" + n.toString(), false, false, null)).collect(toImmutableList()),
                ImmutableList.of(),
                config.returnType.getTypeSignature().getBase(),
                ImmutableList.<String>builder()
                        .add("varchar")
                        .add("varchar")
                        .addAll(IntStream.range(0, config.arity).boxed().map(n -> "T" + n.toString()).collect(toImmutableList()))
                        .build());
        this.scriptingManager = scriptingManager;
        this.config = config;
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
        private final Config config;
        private final List<Type> argTypes;

        public Context(ScriptingManager scriptingManager, Config config, List<Type> argTypes)
        {
            this.scriptingManager = scriptingManager;
            this.config = config;
            this.argTypes = argTypes;
        }
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == config.arity + 2);

        String name = "script_invoker";

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(name),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        // unfucking believable. FUCK OFF PARAMETERIZEDTYPE
        List<Pair<String, Class<?>>> parameters = newArrayList();
        parameters.add(ImmutablePair.of("context", Context.class));
        parameters.add(ImmutablePair.of("session", ConnectorSession.class));
        parameters.add(ImmutablePair.of("scriptingName", Slice.class));
        parameters.add(ImmutablePair.of("functionName", Slice.class));
        for (int i = 0; i < arity - 2; i++) {
            Type argType = boundVariables.getTypeVariable("T" + i);
            Class<?> javaType = argType.getJavaType();
            if (javaType == Void.class) {
                javaType = Object.class; // FUCKING FUCK FUCK YOU
            }
            // javaType = ClassUtils.primitiveToWrapper(javaType);
            parameters.add(ImmutablePair.of("arg" + i, javaType));
        }

        Class returnJavaType = ClassUtils.primitiveToWrapper(
                config.returnType.getJavaType());

        String methodName = "script" + SUFFIX_CHARS_BY_JAVA_TYPE.get(returnJavaType);

        MethodDefinition methodDefinition = definition.declareMethod(a(PUBLIC, STATIC), name, type(returnJavaType), parameters.stream().map(p -> arg(p.getLeft(), p.getRight())).collect(toImmutableList()));
        methodDefinition.declareAnnotation(ScalarFunction.class);
        methodDefinition.declareAnnotation(SqlType.class).setValue("value", config.returnType.getTypeSignature().toString());
        methodDefinition.declareParameterAnnotation(SqlType.class, 2).setValue("value", "varchar");
        methodDefinition.declareParameterAnnotation(SqlType.class, 3).setValue("value", "varchar");
        for (int i = 0; i < arity - 2; i++) {
            methodDefinition.declareParameterAnnotation(SqlType.class, i + 4).setValue("value", boundVariables.getTypeVariable("T" + i).getTypeSignature().toString());
        }

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        BytecodeBlock body = methodDefinition.getBody();

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
                    .push(i);

            Type argType = boundVariables.getTypeVariable("T" + i);
            Class<?> javaType = argType.getJavaType();

            if (javaType == Void.class) {
                body.pushNull(); // FIXME fuq
            }
            else {
                body.getVariable(arg);
                if (javaType == boolean.class) {
                    body.invokeStatic(Boolean.class, "valueOf", Boolean.class, boolean.class);
                }
                else if (javaType == long.class) {
                    body.invokeStatic(Long.class, "valueOf", Long.class, long.class);
                }
                else if (javaType == double.class) {
                    body.invokeStatic(Double.class, "valueOf", Double.class, double.class);
                }
            }

            body.putObjectArrayElement();
        }

        body
                .invokeStatic(ScriptFunction.class, methodName, returnJavaType, Context.class, ConnectorSession.class, Slice.class, Slice.class, Object[].class)
                .retObject();

        Class<?> cls = defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(ScriptFunction.class.getClassLoader()));
        Method method;
        try {
            method = cls.getMethod(name, parameters.stream().map(p -> p.getRight()).collect(toImmutableList()).toArray(new Class<?>[parameters.size()]));
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        List<Type> argTypes = ImmutableList.<Type>builder()
                .add(VarcharType.VARCHAR)
                .add(VarcharType.VARCHAR)
                .addAll(IntStream.range(0, arity - 2).boxed().map(n -> boundVariables.getTypeVariable("T" + n.toString())).collect(toImmutableList()))
                .build();

        MethodHandle methodHandle;
        try {
            methodHandle = lookup().unreflect(method).bindTo(new Context(scriptingManager, config, argTypes));
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return new ScalarFunctionImplementation(true, listOf(arity, true), methodHandle, isDeterministic());
    }

    public static Object script(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        Scripting scripting = context.scriptingManager.getScripting(scriptingName.toStringUtf8());
        Object ret;
        switch (context.config.executionType) {
            case INVOKE:
                ret = scripting.invoke(str.toStringUtf8(), args);
                break;
            case EVAL:
                checkArgument(args.length == 0);
                ret = scripting.eval(str.toStringUtf8());
                break;
            default:
                throw new IllegalArgumentException(Objects.toString(context.config.executionType));
        }
        return ret;
    }

    public static final Map<Class, Character> SUFFIX_CHARS_BY_JAVA_TYPE = ImmutableMap.<Class, Character>builder()
            .put(Boolean.class, 'b')
            .put(Long.class, 'l')
            .put(Double.class, 'd')
            .put(Slice.class, 's')
            .put(Void.class, 'v')
            .build();

    public static Boolean scriptb(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        return (Boolean) script(context, session, scriptingName, str, args);
    }

    public static Long scriptl(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        Number num = (Number) script(context, session, scriptingName, str, args);
        return num != null ? num.longValue() : null;
    }

    public static Double scriptd(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        Number num = (Number) script(context, session, scriptingName, str, args);
        return num != null ? num.doubleValue() : null;
    }

    public static Slice scripts(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        Object ret = script(context, session, scriptingName, str, args);
        if (ret instanceof Slice) {
            return (Slice) ret;
        }
        else if (ret instanceof String) {
            return Slices.utf8Slice((String) ret);
        }
        else if (ret instanceof byte[]) {
            return Slices.wrappedBuffer((byte[]) ret);
        }
        else if (ret == null) {
            return null;
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static Void scriptv(Context context, ConnectorSession session, Slice scriptingName, Slice str, Object... args)
    {
        return (Void) script(context, session, scriptingName, str, args);
    }
}
