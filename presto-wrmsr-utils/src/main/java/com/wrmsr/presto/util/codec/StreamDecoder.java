package com.wrmsr.presto.util.codec;

import java.io.InputStream;

@FunctionalInterface
public interface StreamDecoder
{
    InputStream decode(InputStream data);
}
