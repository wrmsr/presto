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

import java.util.Map;
import java.util.Set;

public interface BatchedMap<K, V> extends Map<K, V>
{
    Map<K, V> getAll(Set<? extends K> keys);

    default Set<K> containsKeys(Set<? extends K> keys)
    {
        return getAll(keys).keySet();
    }

    void putAll(Map<? extends K, ? extends V> map);

    Set<K> removeAll(Set<? extends K> keys);

    class Proxy<K, V> extends MapProxy<K, V> implements BatchedMap<K, V>
    {
        public Proxy(Map<K, V> target)
        {
            super(target);
        }

        @Override
        public Map<K, V> getAll(Set<? extends K> keys)
        {
            return null;
        }

        @Override
        public Set<K> removeAll(Set<? extends K> keys)
        {
            return null;
        }
    }
}
