package com.wrmsr.presto.util.codec;

import java.util.function.Function;

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

    static <F, M, T> Codec<F, T> compose(Codec<F, M> c1, Codec<M, T> c2)
    {
        return new Codec<F, T>() {
            @Override
            public F decode(T data)
            {
                return c1.decode(c2.decode(data));
            }

            @Override
            public T encode(F data)
            {
                return c2.encode(c1.encode(data));
            }
        };
    }

    static <F1, T1, F2, T2> Codec<F1, T2> compose(Codec<F1, T1> c1, Codec<F2, T2> c2, Function<T1, F2> f1, Function<F2, T1> f2)
    {
        return new Codec<F1, T2>() {
            @Override
            public F1 decode(T2 data)
            {
                return c1.decode(f2.apply(c2.decode(data)));
            }

            @Override
            public T2 encode(F1 data)
            {
                return c2.encode(f1.apply(c1.encode(data)));
            }
        };
    }
}
