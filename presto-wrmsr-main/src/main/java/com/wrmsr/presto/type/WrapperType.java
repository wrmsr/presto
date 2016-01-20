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
package com.wrmsr.presto.type;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

// FIXME spi
public interface WrapperType
        extends Type
{
    Type getWrappedType();

    boolean isAnnotaitonType();

    @SuppressWarnings({"unchecked"})
    static <T extends WrapperType> Optional<T> getFirst(Type type, Class<T> cls)
    {
        if (cls.isInstance(type)) {
            return Optional.of((T) type);
        }
        else if (type instanceof WrapperType) {
            return getFirst(((WrapperType) type).getWrappedType(), cls);
        }
        else {
            return Optional.empty();
        }
    }

    @SuppressWarnings({"unchecked"})
    static <T extends WrapperType> List<T> getAll(Type type, Class<T> cls)
    {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        while (type instanceof WrapperType) {
            WrapperType wrapperType = (WrapperType) type;
            if (cls.isInstance(wrapperType)) {
                builder.add((T) wrapperType);
            }
            type = wrapperType.getWrappedType();
        }
        return builder.build();
    }

    static Type stripAnnotations(Type type)
    {
        if (type instanceof WrapperType) {
            WrapperType wrapperType = (WrapperType) type;
            if (wrapperType.isAnnotaitonType()) {
                return stripAnnotations(wrapperType.getWrappedType());
            }
        }
        return type;
    }
}
