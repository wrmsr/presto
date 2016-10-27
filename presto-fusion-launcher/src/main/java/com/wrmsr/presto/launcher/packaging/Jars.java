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
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.wrmsr.presto.launcher.packaging.entries.BytesEntry;
import com.wrmsr.presto.launcher.packaging.entries.DirectoryEntry;
import com.wrmsr.presto.launcher.packaging.entries.Entry;
import com.wrmsr.presto.launcher.packaging.entries.EntryVisitor;
import com.wrmsr.presto.launcher.packaging.entries.FileEntry;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.MoreIO.readFullyAndClose;

public final class Jars
{
    private Jars()
    {
    }

    public static List<Entry> getJarEntries(File inFile)
            throws IOException
    {
        List<Entry> entries = new ArrayList<>();
        Set<String> seenNames = new HashSet<>();
        try (ZipFile zipFile = new ZipFile(inFile)) {
            Enumeration<? extends ZipEntry> zipEntries;
            for (zipEntries = zipFile.entries(); zipEntries.hasMoreElements(); ) {
                ZipEntry zipEntry = zipEntries.nextElement();
                checkState(!seenNames.contains(zipEntry.getName()));
                seenNames.add(zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    entries.add(
                            new DirectoryEntry(
                                    zipEntry.getName(),
                                    zipEntry.getTime()));
                }
                else {
                    byte[] bytes = readFullyAndClose(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize());
                    entries.add(
                            new BytesEntry(
                                    zipEntry.getName(),
                                    zipEntry.getTime(),
                                    bytes));
                }
            }
        }
        return entries;
    }

    public static List<String> getNameDirectories(String name)
    {
        List<String> pathParts = new ArrayList<>(Arrays.asList(name.split("/")));
        StringBuilder sb = new StringBuilder(name.length());
        pathParts.remove(pathParts.size() - 1);
        List<String> ret = new ArrayList<>();
        for (String pathPart : pathParts) {
            sb.append(pathPart);
            sb.append("/");
            ret.add(sb.toString());
        }
        return ret;
    }

    public static void buildJar(List<Entry> entries, File outFile)
            throws IOException
    {
        Set<String> seenNames = new HashSet<>();
        Set<String> seenDirectories = new HashSet<>();
        try (BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outFile));
                JarOutputStream jo = new JarOutputStream(bo)) {
            for (Entry entry : entries) {
                checkState(!seenNames.contains(entry.getName()));
                seenNames.add(entry.getName());

                for (String directory : getNameDirectories((entry.getName()))) {
                    if (!seenDirectories.contains(directory)) {
                        seenDirectories.add(directory);
                        try {
                            JarEntry jarEntry = new JarEntry(directory);
                            jo.putNextEntry(jarEntry);
                            jo.write(new byte[0], 0, 0);
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }

                    entry.accept(new EntryVisitor<Void, Void>()
                    {
                        @Override
                        public Void visitEntry(Entry entry, Void context)
                        {
                            throw new IllegalStateException();
                        }

                        @Override
                        public Void visitBytesEntry(BytesEntry entry, Void context)
                        {
                            try {
                                JarEntry jarEntry = new JarEntry(entry.getName());
                                entry.bestowJarEntryAttributes(jarEntry);
                                jo.write(entry.getBytes(), 0, entry.getBytes().length);
                            }
                            catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                            return null;
                        }

                        @Override
                        public Void visitDirectoryEntry(DirectoryEntry entry, Void context)
                        {
                            return null;
                        }

                        @Override
                        public Void visitFileEntry(FileEntry entry, Void context)
                        {
                            try {
                                JarEntry jarEntry = new JarEntry(entry.getName());
                                entry.bestowJarEntryAttributes(jarEntry);
                                try (BufferedInputStream bi = new BufferedInputStream(new FileInputStream(entry.getFile()))) {
                                    ByteStreams.copy(bi, jo);
                                }
                            }
                            catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                            return null;
                        }
                    }, null);
                }
            }
        }
    }

    public static void makeExecutableJar(File inputFile, File outputFile)
            throws IOException
    {
        checkState(inputFile.isFile());
        checkState(outputFile.getParentFile().isDirectory());

        byte[] launcherBytes;
        try (InputStream launcherStream = OldPackager.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/launcher/packaging/entrypoint")) {
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

        checkState(outputFile.setExecutable(true, false));
    }
}
