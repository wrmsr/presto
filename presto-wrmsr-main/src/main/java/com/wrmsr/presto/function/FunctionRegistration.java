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
package com.wrmsr.presto.function;

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Consumer;

@FunctionalInterface
public interface FunctionRegistration
{
    interface Self extends FunctionRegistration, SqlFunction
    {
        default List<SqlFunction> getFunctions(TypeManager typeManager)
        {
            return ImmutableList.of(this);
        }
    }

    List<SqlFunction> getFunctions(TypeManager typeManager);

    static FunctionRegistration of(List<SqlFunction> functions)
    {
        return new FunctionRegistration() {
            @Override
            public List<SqlFunction> getFunctions(TypeManager typeManager)
            {
                return functions;
            }
        };
    }

    static FunctionRegistration of(Consumer<FunctionListBuilder> fn)
    {
        return new FunctionRegistration() {
            @Override
            public List<SqlFunction> getFunctions(TypeManager typeManager)
            {
                FunctionListBuilder builder = new FunctionListBuilder(typeManager);
                fn.accept(builder);
                return builder.getFunctions();
            }
        };
    }
}
