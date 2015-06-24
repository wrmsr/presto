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

import com.google.common.collect.ForwardingMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CaseInsensitiveMap<V> extends ForwardingMap<String,
        V> implements Serializable
{
    private static final long serialVersionUID = -7741335486707072323L;

    public CaseInsensitiveMap()
    {
        this(new HashMap<String, V>());
    }

    public CaseInsensitiveMap(final Map<String, V> inner)
    {
        this.inner = inner;
    }

    private final Map<String, V> inner;

    @Override
    protected Map<String, V> delegate()
    {
        return inner;
    }

    private static String lower(final Object key)
    {
        return key == null ? null : key.toString().toLowerCase();
    }

    @Override
    public V get(final Object key)
    {
        return inner.get(lower(key));
    }

    @Override
    public void putAll(final Map<? extends String, ? extends V> map)
    {
        if (map == null || map.isEmpty()) {
            inner.putAll(map);
        }
        else {
            for (final Entry<? extends String, ? extends V> entry :
                    map.entrySet()) {
                inner.put(lower(entry.getKey()), entry.getValue());
            }
        }
    }

    @Override
    public V remove(final Object object)
    {
        return inner.remove(lower(object));
    }

    @Override
    public boolean containsKey(final Object key)
    {
        return inner.containsKey(lower(key));
    }

    @Override
    public V put(final String key, final V value)
    {
        return inner.put(lower(key), value);
    }
}
