package com.wrmsr.presto.util.codec;

import java.io.OutputStream;

@FunctionalInterface
public interface StreamEncoder
{
    OutputStream encode(OutputStream data);
}
