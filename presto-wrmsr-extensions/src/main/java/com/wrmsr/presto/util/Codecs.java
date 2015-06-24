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

import java.io.InputStream;
import java.io.OutputStream;

public class Codecs
{
    private Codecs()
    {
    }

    public interface Codec<F, T>
        extends Encoder<F, T>, Decoder<F, T>
    {
        static <F, T> Codec<F, T> of(Encoder<F, T> encoder, Decoder<F, T> decoder)
        {
            return new Codec<F, T>()
            {
                @Override
                public F decode(T data)
                {
                    return decoder.decode(data);
                }

                @Override
                public T encode(F data)
                {
                    return encoder.encode(data);
                }
            };
        }

        static <F, T> Codec<F, T> flip(Codec<T, F> codec)
        {
            return new Codec<F, T>()
            {
                @Override
                public F decode(T data)
                {
                    return codec.encode(data);
                }

                @Override
                public T encode(F data)
                {
                    return codec.decode(data);
                }
            };
        }
    }

    @FunctionalInterface
    public interface Encoder<F, T>
    {
        T encode(F data);
    }

    @FunctionalInterface
    public interface Decoder<F, T>
    {
        F decode(T data);
    }

    // TODO: composite - can this POSSIBLY be done type safely lololol

    public interface StreamCodec
        extends StreamEncoder, StreamDecoder
    {
    }

    @FunctionalInterface
    public interface StreamEncoder
    {
        OutputStream encode(OutputStream data);
    }

    @FunctionalInterface
    public interface StreamDecoder
    {
        InputStream decode(InputStream data);
    }
}
