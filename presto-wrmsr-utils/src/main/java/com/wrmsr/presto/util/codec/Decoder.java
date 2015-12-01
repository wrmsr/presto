package com.wrmsr.presto.util.codec;

@FunctionalInterface
public interface Decoder<F, T>
{
    F decode(T data);
}
