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
package com.wrmsr.presto.function;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileFunctions
{
    @ScalarFunction("read_file")
    @Description("reads file")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice isNull(@SqlType(StandardTypes.VARCHAR) Slice path)
    {
        File file = new File(path.toStringUtf8());
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file.toPath());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return Slices.wrappedBuffer(bytes);
    }

    @ScalarFunction("write_file")
    @Description("write file")
    @SqlType(StandardTypes.BIGINT)
    public static long isNull(@SqlType(StandardTypes.VARCHAR) Slice path, @SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        File file = new File(path.toStringUtf8());
        try {
            Files.write(file.toPath(), slice.getBytes());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return slice.length();
    }
}
