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
package com.wrmsr.presto.struct;

import static java.util.Objects.requireNonNull;

public abstract class Field
{
    private final AliasedName aliasedName;
    private final Type type;

    public Field(AliasedName aliasedName, Type type)
    {
        this.aliasedName = requireNonNull(aliasedName);
        this.type = requireNonNull(type);
    }

    public AliasedName getAliasedName()
    {
        return aliasedName;
    }

    public Type getType()
    {
        return type;
    }
}
