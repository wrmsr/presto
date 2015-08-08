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

import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;

public class Files
{
    private Files()
    {
    }

    public static String readFile(String path) throws IOException
    {
        try (BufferedReader br = java.nio.file.Files.newBufferedReader(
                FileSystems.getDefault().getPath(path),
                StandardCharsets.UTF_8)) {
            return CharStreams.toString(br);
        }
    }

    public static void downloadFile(String url, File path) throws IOException
    {
        try (ReadableByteChannel rbc = Channels.newChannel(new URL(url).openStream());
             FileOutputStream fos = new FileOutputStream(path)) {
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        }
    }
}
