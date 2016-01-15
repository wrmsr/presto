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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

public class JacksonTypeCodec
    extends TypeCodec
{
    private final ObjectMapper mapper;

    public JacksonTypeCodec(String name, ObjectMapper mapper)
    {
        super(name);
        this.mapper = mapper;
    }

    @Override
    public <T> Codec<T, Slice> getCodec(Type fromType)
    {
        return null;
    }
}