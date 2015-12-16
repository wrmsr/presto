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

import java.util.Arrays;

import com.wrmsr.presto.util.codec.Codec;

public class ByteArrayWrapper
{
    public static final Codec<byte[], ByteArrayWrapper> CODEC = Codec.of(
            ByteArrayWrapper::new,
            ByteArrayWrapper::getData
    );

    private final byte[] data;

    public ByteArrayWrapper(byte[] data)
    {
        if (data == null) {
            throw new NullPointerException();
        }
        this.data = data;
    }

    public byte[] getData()
    {
        return data;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper)) {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper) other).data);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }
}
