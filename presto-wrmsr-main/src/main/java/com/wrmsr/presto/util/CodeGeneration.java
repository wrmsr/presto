package com.wrmsr.presto.util;

import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;

public class CodeGeneration
{
    public static Class<?> generateBox(String name, Class<?> valueClass)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(name + "$box"),
                type(Box.class, valueClass));

        MethodDefinition methodDefinition = definition.declareConstructor(a(PUBLIC), ImmutableList.of(arg("value", valueClass)));
        methodDefinition.getBody()
                .getVariable(methodDefinition.getThis())
                .getVariable(methodDefinition.getScope().getVariable("value"))
                .invokeConstructor(Box.class, Object.class)
                .ret();

        return defineClass(definition, Object.class, new CallSiteBinder().getBindings(), new DynamicClassLoader(CodeGeneration.class.getClassLoader()));
    }
}
