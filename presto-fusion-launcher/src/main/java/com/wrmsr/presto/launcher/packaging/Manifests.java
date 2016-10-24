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
package com.wrmsr.presto.launcher.packaging;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.MoreIO.readFullyAndClose;

public final class Manifests
{
    private Manifests()
    {
    }

    public static Map<String, String> getManifest(File file)
            throws IOException
    {
        byte[] bytes;
        try (ZipFile zipFile = new ZipFile(file)) {
            ZipEntry zipEntry = zipFile.getEntry("META-INF/MANIFEST.MF");
            if (zipEntry == null) {
                return new HashMap<>();
            }
            bytes = readFullyAndClose(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize());
        }
        return parseManifest(bytes);
    }

    public static Map<String, String> parseManifest(byte[] bytes)
    {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Manifest_Specification
        Map<String, String> manifest = new LinkedHashMap<>();

        String contents = new String(bytes, StandardCharsets.UTF_8);
        List<String> lines = new ArrayList<>(Splitter.on("\r\n").splitToList(contents));
        for (int i = 0; i < 2; ++i) {
            checkState(!lines.isEmpty());
            checkState(lines.get(lines.size() - 1).isEmpty());
            lines.remove(lines.size() - 1);
        }
        for (String line : lines) {
            List<String> split = Splitter.on(": ").splitToList(line);
            checkState(split.size() == 2);
            checkState(!manifest.containsKey(split.get(0)));
            manifest.put(split.get(0), split.get(1));
        }

        return manifest;
    }

    private static final List<Character> BAD_CHARS = ImmutableList.of(':', '\r', '\n');

    public static byte[] renderManifest(Map<String, String> manifest)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : manifest.entrySet()) {
            for (char c : BAD_CHARS) {
                checkState(entry.getKey().indexOf(c) < 0);
                checkState(entry.getValue().indexOf(c) < 0);
            }
            sb.append(String.format("%s: %s\r\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
