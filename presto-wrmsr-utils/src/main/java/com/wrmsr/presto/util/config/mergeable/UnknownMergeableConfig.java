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
package com.wrmsr.presto.util.config.mergeable;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Mergeable;

import java.util.Map;

public abstract class UnknownMergeableConfig<N extends UnknownMergeableConfig<N>>
        implements MergeableConfig<N>
{
    private final String type;
    private final Object object;

    public UnknownMergeableConfig()
    {
        throw new UnsupportedOperationException();
    }

    public UnknownMergeableConfig(String type, Object object)
    {
        this.type = type;
        this.object = object;
    }

    public String getType()
    {
        return type;
    }

    public Object getObject()
    {
        return object;
    }

    @JsonValue
    public Map<String, Object> jsonValue()
    {
        return ImmutableMap.of(type, object);
    }

    @Override
    public Mergeable merge(Mergeable other)
    {
        throw new UnsupportedOperationException();
    }
}
