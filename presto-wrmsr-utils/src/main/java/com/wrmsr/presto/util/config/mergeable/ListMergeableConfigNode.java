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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Mergeable;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public abstract class ListMergeableConfigNode<N extends ListMergeableConfigNode<N, T>, T>
    implements MergeableConfigNode<N>, Iterable<T>
{
    @SuppressWarnings({"unchecked"})
    public static <T> List<T> unpack(Object object, Class<T> cls)
    {
        if (object instanceof List) {
            return (List<T>) object;
        }
        else if (cls.isInstance(object)) {
            return ImmutableList.of((T) object);
        }
        else {
            throw new IllegalArgumentException(Objects.toString(object));
        }
    }

    protected final List<T> items;

    public ListMergeableConfigNode()
    {
        this.items = ImmutableList.of();
    }

    public ListMergeableConfigNode(List<T> items)
    {
        this.items = ImmutableList.copyOf(items);
    }

    public List<T> getItems()
    {
        return items;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "{" +
                "items=" + items +
                '}';
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Mergeable merge(Mergeable other)
    {
        List mergedList = ImmutableList.builder()
                .addAll(items)
                .addAll(((N) other).getItems())
                .build();
        N merged;
        try {
            merged = (N) getClass().getConstructor(List.class).newInstance(mergedList);
        }
        catch (IllegalAccessException | NoSuchMethodException | InstantiationException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
        return merged;
    }

    @Override
    public Iterator<T> iterator()
    {
        return items.iterator();
    }
}
