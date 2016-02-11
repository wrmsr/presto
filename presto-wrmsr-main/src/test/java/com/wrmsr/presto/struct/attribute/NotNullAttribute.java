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
package com.wrmsr.presto.struct.attribute;

import com.wrmsr.presto.struct.Attribute;
import com.wrmsr.presto.struct.Field;
import com.wrmsr.presto.struct.Struct;

import java.util.Optional;

public final class NotNullAttribute
        implements Attribute<NotNullAttribute>
{
    @Override
    public Optional<Field> getField()
    {
        return null;
    }

    @Override
    public void validate(Struct struct)
    {

    }

    @Override
    public NotNullAttribute merge(NotNullAttribute other)
    {
        return this;
    }
}
