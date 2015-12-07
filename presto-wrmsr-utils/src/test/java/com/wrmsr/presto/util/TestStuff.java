package com.wrmsr.presto.util;

import com.wrmsr.presto.util.codec.Codec;
import org.testng.annotations.Test;

public class TestStuff
{
    @Test
    public void testStuff() throws Throwable
    {
        Codec<Integer, Integer> add2 = Codec.of(i -> i + 2, i -> i - 2);
        Codec<Integer, Integer> mul3 = Codec.of(i -> i * 3, i -> i / 3);
        Codec<Integer, Integer> cii = Codec.compose(add2, mul3);
        cii.encode(10);

        Codec<Float, Integer> toInt = Codec.of(f -> (int) (float) f, i -> (float) i);
        Codec<Float, Integer> cfi = Codec.compose(toInt, add2);
        cfi.encode(10.0f);
    }
}
