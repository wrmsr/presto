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

import com.google.common.base.Throwables;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.wrmsr.presto.util.MoreIO.readFullyAndClose;

public final class Manifests
{
    private Manifests()
    {
    }

    public static Manifest getManifest(File file)
            throws IOException
    {
        byte[] bytes;
        try (ZipFile zipFile = new ZipFile(file)) {
            ZipEntry zipEntry = zipFile.getEntry("META-INF/MANIFEST.MF");
            if (zipEntry == null) {
                return new Manifest();
            }
            bytes = readFullyAndClose(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize());
        }
        return parseManifest(bytes);
    }

    public static Manifest parseManifest(byte[] bytes)
    {
        Manifest manifest = new Manifest();
        try {
            manifest.read(new ByteArrayInputStream(bytes));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return manifest;
    }

    public static byte[] renderManifest(Manifest manifest)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            manifest.write(out);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return out.toByteArray();
    }
}
