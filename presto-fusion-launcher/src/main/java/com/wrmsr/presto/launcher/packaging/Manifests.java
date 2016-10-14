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
import com.google.common.io.CharStreams;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;

public class Manifests
{
    private static Map<String, String> getManifest(File file)
            throws IOException
    {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html#Manifest_Specification
        Map<String, String> manifest = new LinkedHashMap<>();
        ZipFile zf = new ZipFile(file);
        for (Enumeration<? extends ZipEntry> zipEntries = zf.entries(); zipEntries.hasMoreElements(); ) {
            ZipEntry ze = zipEntries.nextElement();
            if (ze.getName().equals("META-INF/MANIFEST.MF")) {
                String contents = CharStreams.toString(new InputStreamReader(zf.getInputStream(ze)));

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
            }
        }
        return manifest;
    }
}
