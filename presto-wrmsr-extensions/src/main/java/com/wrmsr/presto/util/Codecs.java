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
    }

    @FunctionalInterface
    public interface Decoder<F, T>
    {
        F decode(T data);
    }

    @FunctionalInterface
    public interface Encoder<F, T>
    {
        T encode(F data);
    }

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
