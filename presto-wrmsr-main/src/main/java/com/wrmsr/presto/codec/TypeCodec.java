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
package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.Type;
import com.wrmsr.presto.util.codec.Codec;

import static java.util.Objects.requireNonNull;

public abstract class TypeCodec<J>
{
    protected final String name;
    protected final Class<J> javaType;

    protected TypeCodec(String name, Class<J> javaType)
    {
        this.name = name;
        this.javaType = javaType;
    }

    public String getName()
    {
        return name;
    }

    public Class<J> getJavaType()
    {
        return javaType;
    }

    public static final class Specialization<T, J>
    {
        private final Codec<T, J> codec;
        private final Type underlyingType;
        private final boolean isAnnotation;

        public Specialization(Codec<T, J> codec, Type underlyingType, boolean isAnnotation)
        {
            this.codec = requireNonNull(codec);
            this.underlyingType = requireNonNull(underlyingType);
            this.isAnnotation = isAnnotation;
        }

        public Codec<T, J> getCodec()
        {
            return codec;
        }

        public Type getUnderlyingType()
        {
            return underlyingType;
        }

        public boolean isAnnotation()
        {
            return isAnnotation;
        }
    }

    public abstract <T> Specialization<T, J> specialize(Type fromType);
}
