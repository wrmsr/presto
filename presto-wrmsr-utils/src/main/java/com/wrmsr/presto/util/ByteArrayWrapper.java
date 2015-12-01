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
