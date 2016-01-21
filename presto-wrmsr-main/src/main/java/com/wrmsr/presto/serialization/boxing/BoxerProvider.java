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
package com.wrmsr.presto.serialization.boxing;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.type.WrapperType;

import java.util.Map;
import java.util.Optional;

import static com.wrmsr.presto.util.collect.Maps.immutableMapWith;

public interface BoxerProvider
{
    final class Context
    {
        private final Map<Class<?>, Object> entries;

        public Context()
        {
            this(ImmutableMap.of());
        }

        // FIXME stack?

        public Context(Map<Class<?>, Object> entries)
        {
            this.entries = entries;
        }

        public Context with(Object entry)
        {
            return new Context(immutableMapWith(entries, entry.getClass(), entry));
        }
    }

    final class WrapperMarker
        implements BoxerProvider
    {
        private final Class<? extends WrapperType> cls;

        public WrapperMarker(Class<? extends WrapperType> cls)
        {
            this.cls = cls;
        }

        // FIXME RECIRCULATE???
        @Override
        public Optional<Boxer> getBoxer(Type type, Context context)
        {
            throw new UnsupportedOperationException();
        }
    }

    Optional<Boxer> getBoxer(Type type, Context context);

    default Optional<Boxer> getBoxer(Type type)
    {
        return getBoxer(type, new Context());
    }
}
