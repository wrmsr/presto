package com.wrmsr.presto.util.codec;

import com.wrmsr.presto.util.Codecs;

public interface Codec<F, T>
    extends Encoder<F, T>, Decoder<F, T>
{
    // TODO: composite - can this POSSIBLY be done type safely lololol

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
