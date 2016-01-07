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
package com.wrmsr.presto.util;

import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.bytecode.CompilerUtils;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;

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
