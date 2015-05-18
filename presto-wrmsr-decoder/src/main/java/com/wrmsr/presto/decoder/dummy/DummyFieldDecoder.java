/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.decoder.dummy;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import com.wrmsr.presto.decoder.DecoderColumnHandle;
import com.wrmsr.presto.decoder.FieldDecoder;
import com.wrmsr.presto.decoder.FieldValueProvider;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.wrmsr.presto.decoder.ErrorCode.CONVERSION_NOT_SUPPORTED;
import static java.lang.String.format;

/**
 * Default 'decoder' for the dummy format. Can not decode anything. This is intentional.
 */
public class DummyFieldDecoder
        implements FieldDecoder<Void>
{
    @Override
    public Set<Class<?>> getJavaTypes()
    {
        return ImmutableSet.<Class<?>>of(boolean.class, long.class, double.class, Slice.class);
    }

    @Override
    public final String getRowDecoderName()
    {
        return DummyRowDecoder.NAME;
    }

    @Override
    public String getFieldDecoderName()
    {
        return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
    }

    @Override
    public FieldValueProvider decode(Void value, DecoderColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");

        return new FieldValueProvider()
        {
            @Override
            public boolean accept(DecoderColumnHandle handle)
            {
                return false;
            }

            @Override
            public boolean isNull()
            {
                throw new PrestoException(CONVERSION_NOT_SUPPORTED, "is null check not supported");
            }
        };
    }

    @Override
    public String toString()
    {
        return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
    }
}
