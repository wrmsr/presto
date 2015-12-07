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

import java.util.List;
import java.util.Optional;

@FunctionalInterface
public interface TypeCodecProvider
{
    Optional<TypeCodec> getTypeCodec(String name, Type fromType);

    static TypeCodecProvider of(List<TypeCodec> typeCodecs)
    {
        return (name, fromType) -> typeCodecs.stream().filter(c -> c.getName().equals(name) && c.getFromType().equals(fromType)).findFirst();
    }
}
