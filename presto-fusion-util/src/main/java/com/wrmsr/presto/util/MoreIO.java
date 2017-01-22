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
package com.wrmsr.presto.util;

import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import static com.google.common.base.Preconditions.checkState;

public final class MoreIO
{
    private MoreIO()
    {
    }

    public static byte[] readFullyAndClose(InputStream is, int len)
            throws IOException
    {
        try {
            byte[] bytes;
            bytes = new byte[len];
            ByteStreams.readFully(is, bytes);
            checkState(is.read() == -1);
            return bytes;
        }
        finally {
            is.close();
        }
    }

    public static void deleteDirectory(Path rootPath)
            throws IOException
    {
        Files.walk(rootPath)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}
