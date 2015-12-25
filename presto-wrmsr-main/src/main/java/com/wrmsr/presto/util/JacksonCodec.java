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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.wrmsr.presto.util.codec.Codec;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public class JacksonCodec<T>
        implements Codec<T, byte[]>
{
    private final ObjectMapper mapper;
    private final Class<T> cls;

    public JacksonCodec(ObjectMapper mapper, Class<T> cls)
    {
        this.mapper = mapper;
        this.cls = cls;
    }

    @Override
    public T decode(byte[] data)
    {
        try {
            return mapper.readValue(data, cls);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public byte[] encode(T data)
    {
        checkArgument(data == null || cls.isInstance(data));
        try {
            return mapper.writeValueAsBytes(data);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
