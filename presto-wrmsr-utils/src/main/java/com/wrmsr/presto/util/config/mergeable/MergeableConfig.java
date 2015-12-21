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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.Mergeable;
import com.wrmsr.presto.util.Serialization;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;

public interface MergeableConfig<N extends MergeableConfig>
        extends Mergeable
{
    @SuppressWarnings({"unchecked"})
    @Override
    default Mergeable merge(Mergeable other)
    {
        ObjectMapper mapper = Serialization.OBJECT_MAPPER.get();
        Map<String, Map> m = Serialization.roundTrip(mapper, this, Map.class);
        String name = Iterables.getOnlyElement(m.keySet());
        Map<String, Object> newMap = newHashMap(Iterables.getOnlyElement(m.values()));
        m = Serialization.roundTrip(mapper, other, Map.class);
        checkArgument(name.equals(Iterables.getOnlyElement(m.keySet())));
        Map<String, Object> otherMap = Iterables.getOnlyElement(m.values());
        for (Map.Entry<String, Object> entry : otherMap.entrySet()) {
            newMap.put(entry.getKey(), entry.getValue());
        }
        return Serialization.roundTrip(mapper, otherMap, (Class<N>) getClass());
    }
}
