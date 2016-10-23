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

import com.google.common.io.CharStreams;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkState;

public final class Jars
{
    private Jars()
    {
    }

    public static void makeExecutableJar(File inputFile, File outputFile)
            throws IOException
    {
        checkState(inputFile.isFile());
        checkState(outputFile.getParentFile().isDirectory());

        byte[] launcherBytes;
        try (InputStream launcherStream = Packager.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/launcher/packaging/entrypoint")) {
            launcherBytes = CharStreams.toString(new InputStreamReader(launcherStream, StandardCharsets.UTF_8)).getBytes();
        }

        try (InputStream fi = new BufferedInputStream(new FileInputStream(inputFile));
                OutputStream fo = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            fo.write(launcherBytes, 0, launcherBytes.length);
            fo.write(new byte[] {'\n', '\n'});
            byte[] buf = new byte[65536];
            int anz;
            while ((anz = fi.read(buf)) != -1) {
                fo.write(buf, 0, anz);
            }
        }

        outputFile.setExecutable(true, false);
    }
}
