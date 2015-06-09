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
                public T encode(F data)
                {
                    return encoder.encode(data);
                }

                @Override
                public F decode(T data)
                {
                    return decoder.decode(data);
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
