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
import com.facebook.presto.spi.type.VarbinaryType;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.wrmsr.presto.util.SliceCodecs.SLICE_TO_BYTES_CODEC;

public class CompressionTypeCodec
        extends SliceTypeCodec
{
    private final Codec<byte[], byte[]> bytesCodec;
    private final Codec<Slice, Slice> sliceCodec;

    public CompressionTypeCodec(String name, Codec<byte[], byte[]> bytesCodec)
    {
        super(name);
        this.bytesCodec = bytesCodec;
        this.sliceCodec = Codec.compose(
                SLICE_TO_BYTES_CODEC,
                bytesCodec,
                Codec.flip(SLICE_TO_BYTES_CODEC));
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public <T> Codec<T, Slice> specializeCodec(Type fromType)
    {
        checkArgument(fromType.equals(VarbinaryType.VARBINARY));
        return (Codec<T, Slice>) sliceCodec;
    }
}
