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

import com.google.common.base.Throwables;

public interface Mergeable<T extends Mergeable<T>>
{
    T merge(T other);

    default T merge(Iterable<T> others)
    {
        @SuppressWarnings({"unchecked"})
        T t = (T) this;
        for (T o : others) {
            t = t.merge(o);
        }
        return t;
    }

    default T merge(T first, T... rest)
    {
        @SuppressWarnings({"unchecked"})
        T t = (T) this;
        t = t.merge(first);
        for (int i = 0; i < rest.length; ++i) {
            t = t.merge(rest[i]);
        }
        return t;
    }

    static <T extends Mergeable<T>> T unit(Class<T> cls)
    {
        try {
            return cls.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }
}
