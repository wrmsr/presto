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
