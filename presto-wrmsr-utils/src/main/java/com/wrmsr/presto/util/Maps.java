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

import com.google.common.base.Function;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class Maps
{
    private Maps()
    {
    }

    @FunctionalInterface
    public interface MapPutter
    {
        void put(Map map, Object key, Object value);

        MapPutter OVERWRITE = (map, key, value) -> map.put(key, value);

        MapPutter IGNORE = (map, key, value) -> {
            if (!map.containsKey(key)) {
                map.put(key, value);
            }
        };

        class DuplicateItemException extends IllegalStateException
        {
        }

        MapPutter THROW = (map, key, value) -> {
            if (!map.containsKey(key)) {
                map.put(key, value);
            }
            else {
                throw new DuplicateItemException();
            }
        };

        MapPutter LIST = (map, key, value) -> {
            if (!map.containsKey(key)) {
                map.put(key, value);
            }
        };
    }

    public static <K, V> void mapPutAll(Map<K, V> map, Iterator<Map.Entry<K, V>> entries, MapPutter putter)
    {
        while (entries.hasNext()) {
            Map.Entry<K, V> entry = entries.next();
            putter.put(map, entry.getKey(), entry.getValue());
        }
    }

    public static <K, V> void mapPutAll(Map<K, V> map, Iterable<Map.Entry<K, V>> entries, MapPutter putter)
    {
        mapPutAll(map, entries.iterator(), putter);
    }

    public static <K, V> void mapPutAll(Map<K, V> map, Iterable<Map.Entry<K, V>> entries)
    {
        mapPutAll(map, entries, MapPutter.OVERWRITE);
    }

    public static <K, E> Map<K, E> buildMap(Iterator<E> items, Function<E, K> fn, MapPutter putter)
    {
        Map<K, E> map = newHashMap();
        while (items.hasNext()) {
            E item = items.next();
            K key = fn.apply(item);
            putter.put(map, key, item);
        }
        return map;
    }

    public static <K, E> Map<K, E> buildMap(Iterable<E> items, Function<E, K> fn, MapPutter putter)
    {
        return buildMap(items.iterator(), fn, putter);
    }

    public static <K, E> Map<K, E> buildMap(Iterable<E> items, Function<E, K> fn)
    {
        return buildMap(items, fn, MapPutter.OVERWRITE);
    }

    public static <K, V> Map<K, V> buildMap(Iterator<Map.Entry<K, V>> entries, MapPutter putter)
    {
        Map<K, V> map = newHashMap();
        mapPutAll(map, entries, putter);
        return map;
    }

    public static <K, V> Map<K, V> buildMap(Iterable<Map.Entry<K, V>> entries, MapPutter putter)
    {
        return buildMap(entries.iterator(), putter);
    }

    public static <K, V> Map<K, V> buildMap(Iterable<Map.Entry<K, V>> entries)
    {
        return buildMap(entries, MapPutter.OVERWRITE);
    }

    public static <K, E> Map<K, List<E>> buildMapToList(Iterator<E> items, Function<E, K> fn)
    {
        Map<K, List<E>> map = newHashMap();
        while (items.hasNext()) {
            E item = items.next();
            K key = fn.apply(item);
            List<E> list = map.get(key);
            if (list == null) {
                list = newArrayList();
                map.put(key, list);
            }
            list.add(item);
        }
        return map;
    }

    public static <K, E> Map<K, List<E>> buildMapToList(Iterable<E> items, Function<E, K> fn)
    {
        return buildMapToList(items.iterator(), fn);
    }

    public static <K, V> Map<K, V> mapMerge(Map<K, V>... maps)
    {
        Map<K, V> out = newHashMap();
        for (Map<K, V> map : maps) {
            mapPutAll(out, map.entrySet());
        }
        return out;
    }
}
