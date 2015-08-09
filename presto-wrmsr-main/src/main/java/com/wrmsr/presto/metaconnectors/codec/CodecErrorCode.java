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
package com.wrmsr.presto.metaconnectors.codec;

import com.facebook.presto.spi.ErrorCodeSupplier;

/**
 * Kafka connector specific error codes.
 */
public enum CodecErrorCode
        implements ErrorCodeSupplier
{
    // Connectors can use error codes starting at EXTERNAL

    /**
     * A requested data conversion is not supported.
     */
    CONVERSION_NOT_SUPPORTED(0x0200_0000),
    SPLIT_ERROR(0x0200_0001);

    private final com.facebook.presto.spi.ErrorCode errorCode;

    CodecErrorCode(int code)
    {
        errorCode = new com.facebook.presto.spi.ErrorCode(code, name());
    }

    @Override
    public com.facebook.presto.spi.ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
