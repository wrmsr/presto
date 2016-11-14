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
import com.google.common.io.Files;
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

        File tmpDir = Files.createTempDir();
        tmpDir.deleteOnExit();
        File tmpOutputFile = new File(tmpDir, "jar");

        Set<String> beforeNames = JarBuilder.getZipEntryNames(inputFile);

        byte[] launcherBytes;
        try (InputStream launcherStream = OldPackager.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/launcher/packaging/entrypoint")) {
            launcherBytes = CharStreams.toString(new InputStreamReader(launcherStream, StandardCharsets.UTF_8)).getBytes();
        }

        try (InputStream fi = new BufferedInputStream(new FileInputStream(inputFile));
                OutputStream fo = new BufferedOutputStream(new FileOutputStream(tmpOutputFile))) {
            fo.write(launcherBytes, 0, launcherBytes.length);
            ByteStreams.copy(fi, fo);
        }

        try (RandomAccessFile file = new RandomAccessFile(tmpOutputFile, "rws")) {
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
                int zip64EndOfCentralDirectoryOffsetPos = zip64EndOfCentralDirectoryLocatorPos + 8;
                long zip64EndOfCentralDirectoryOffset = buf.getLong(zip64EndOfCentralDirectoryOffsetPos); // ecrec
                checkState(zip64EndOfCentralDirectoryOffset < file.length());
                zip64EndOfCentralDirectoryOffset += launcherBytes.length;

                checkState(buf.getInt((int) zip64EndOfCentralDirectoryOffset) == 0x06064b50);
                buf.putLong(zip64EndOfCentralDirectoryOffsetPos, zip64EndOfCentralDirectoryOffset);

                int zip64OffsetOfStartOfCentralDirectoryPos = (int) zip64EndOfCentralDirectoryOffset + 48;
                long zip64OffsetOfStartOfCentralDirectory = buf.getLong(zip64OffsetOfStartOfCentralDirectoryPos);
                checkState(zip64OffsetOfStartOfCentralDirectory < zip64EndOfCentralDirectoryOffset);
                checkState(buf.getInt(endOfCentralDirectoryPos + 16) == (int) zip64OffsetOfStartOfCentralDirectory);
                zip64OffsetOfStartOfCentralDirectory += launcherBytes.length;
                checkState(buf.getInt((int) zip64OffsetOfStartOfCentralDirectory) == 0x02014b50);
                buf.putInt(endOfCentralDirectoryPos + 16, (int) zip64OffsetOfStartOfCentralDirectory);
                buf.putLong(zip64OffsetOfStartOfCentralDirectoryPos, zip64OffsetOfStartOfCentralDirectory);
            }
        }

        Set<String> afterNames = JarBuilder.getZipEntryNames(tmpOutputFile);
        checkState(beforeNames.equals(afterNames));
        checkState(tmpOutputFile.setExecutable(true, false));
        checkState(tmpOutputFile.renameTo(outputFile));
    }
}
