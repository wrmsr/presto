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
package com.wrmsr.presto.functions;

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayOutputStream;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkArgument;

public class CompressionFunctions
{
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice zlib_decompress(@SqlType(StandardTypes.VARBINARY) Slice compressedData)
    {
        try {
            Inflater inflater = new Inflater();
            inflater.setInput(compressedData.getBytes());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(compressedData.length());
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            byte[] output = outputStream.toByteArray();
            return Slices.wrappedBuffer(output);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
