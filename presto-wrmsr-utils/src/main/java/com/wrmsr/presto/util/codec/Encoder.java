package com.wrmsr.presto.util.codec;

@FunctionalInterface
public interface Encoder<F, T>
{
    T encode(F data);
}
