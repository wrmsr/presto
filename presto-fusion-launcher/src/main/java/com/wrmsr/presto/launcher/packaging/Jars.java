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

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.wrmsr.presto.launcher.packaging.jarBuilder.JarBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Set;

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
        checkState(inputFile.length() < Integer.MAX_VALUE);
        checkState(outputFile.getParentFile().isDirectory());

        Set<String> beforeNames = JarBuilder.getZipEntryNames(inputFile);

        byte[] launcherBytes;
        try (InputStream launcherStream = OldPackager.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/launcher/packaging/entrypoint")) {
            launcherBytes = CharStreams.toString(new InputStreamReader(launcherStream, StandardCharsets.UTF_8)).getBytes();
        }

        try (InputStream fi = new BufferedInputStream(new FileInputStream(inputFile));
                OutputStream fo = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            fo.write(launcherBytes, 0, launcherBytes.length);
            ByteStreams.copy(fi, fo);
        }

        try (RandomAccessFile file = new RandomAccessFile(outputFile, "rws")) {
            MappedByteBuffer buf = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, file.length());
            buf.order(ByteOrder.nativeOrder());

            // https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT

            int endOfCentralDirectoryPos = -1;
            for (int pos = buf.capacity() - 4; pos > buf.capacity() - 65536; --pos) {
                int i = buf.getInt(pos);
                if (i == 0x06054b50) {
                    endOfCentralDirectoryPos = pos;
                    break;
                }
            }
            checkState(endOfCentralDirectoryPos >= 0);

            int zip64EndOfCentralDirectoryLocatorPos = endOfCentralDirectoryPos - 20;
            if (buf.getInt(zip64EndOfCentralDirectoryLocatorPos) == 0x07064b50) {
                int locoffPos = zip64EndOfCentralDirectoryLocatorPos + 8;
                long oldLocoff = buf.getLong(locoffPos);
                checkState(oldLocoff < file.length());
                buf.putLong(locoffPos, oldLocoff + launcherBytes.length);
            }
        }

        Set<String> afterNames = JarBuilder.getZipEntryNames(outputFile);
        checkState(beforeNames.equals(afterNames));

        checkState(outputFile.setExecutable(true, false));
    }
}
