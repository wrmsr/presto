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

import com.google.common.base.Joiner;
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
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

        checkState(outputFile.setExecutable(true, false));
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
                                    zipEntry.getComment(),
                                    zipEntry.getTime()));
                }
                else {
                    byte[] bytes;
                    try (InputStream is = zipFile.getInputStream(zipEntry);
                            BufferedInputStream bi = new BufferedInputStream(is)) {
                        bytes = new byte[(int) zipEntry.getSize()];
                        ByteStreams.readFully(bi, bytes);
                        checkState(bi.read() == -1);
                    }
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

    }

    public static void buildJar(List<Entry> entries, File outFile)
            throws IOException
    {
        Set<String> seenNames = new HashSet<>();
        Set<String> seenDirectories = new HashSet<>();
        try (BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outFile));
                JarOutputStream jo = new JarOutputStream(bo)) {
            Consumer<String> ensureDirectoryExists = (name) -> {
                List<String> pathParts = new ArrayList<>(Arrays.asList(name.split("/")));
                for (int i = 0; i < pathParts.size() - 1; ++i) {
                    String pathPart = Joiner.on("/").join(IntStream.rangeClosed(0, i).boxed().map(j -> pathParts.get(j)).collect(Collectors.toList())) + "/";
                    if (!seenDirectories.contains(pathPart)) {
                        seenDirectories.add(pathPart);
                        try {
                            JarEntry jarEntry = new JarEntry(pathPart);
                            jo.putNextEntry(jarEntry);
                            jo.write(new byte[] {}, 0, 0);
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }
            };
            for (Entry entry : entries) {
                checkState(!seenNames.contains(entry.getName()));
                seenNames.add(entry.getName());
                entry.accept(new EntryVisitor<Void, Void>()
                {
                    @Override
                    public Void visitEntry(Entry entry, Void context)
                    {
                        throw new IllegalStateException();
                    }

                    @Override
                    public Void visitBytesEntry(Entry entry, Void context)
                    {
                        return super.visitBytesEntry(entry, context);
                    }

                    @Override
                    public Void visitDirectoryEntry(Entry entry, Void context)
                    {
                        ensureDirectoryExists.accept(entry.getName());
                        return null;
                    }

                    @Override
                    public Void visitFileEntry(Entry entry, Void context)
                    {
                        return super.visitFileEntry(entry, context);
                    }
                }, null);
            }
        }

        Set<String> contents = new HashSet<>();
        ZipFile wrapperJarZip = new ZipFile(wrapperJarFile);
        Enumeration<? extends ZipEntry> zipEntries;
        for (zipEntries = wrapperJarZip.entries(); zipEntries.hasMoreElements(); ) {
            ZipEntry zipEntry = zipEntries.nextElement();
            BufferedInputStream bi = new BufferedInputStream(wrapperJarZip.getInputStream(zipEntry));
            JarEntry je = new JarEntry(zipEntry.getName());
            jo.putNextEntry(je);
            byte[] buf = new byte[1024];
            int anz;
            while ((anz = bi.read(buf)) != -1) {
                jo.write(buf, 0, anz);
            }
            bi.close();
            contents.add(zipEntry.getName());
        }

        for (String key : keys) {
            if (contents.contains(key)) {
                log.warn(key);
                continue;
            }
            Entry e = entryMap.get(key);
            String p = e.getName();
            List<String> pathParts = new ArrayList<>(Arrays.asList(p.split("/")));
            for (int i = 0; i < pathParts.size() - 1; ++i) {
                String pathPart = Joiner.on("/").join(IntStream.rangeClosed(0, i).boxed().map(j -> pathParts.get(j)).collect(Collectors.toList())) + "/";
                if (!contents.contains(pathPart)) {
                    JarEntry je = new JarEntry(pathPart);
                    jo.putNextEntry(je);
                    jo.write(new byte[] {}, 0, 0);
                    contents.add(pathPart);
                }
            }
            JarEntry je = new JarEntry(p);
            e.bestowJarEntryAttributes(je);
            jo.putNextEntry(je);
            if (e instanceof FileEntry) {
                FileEntry f = (FileEntry) e;
                BufferedInputStream bi = new BufferedInputStream(new FileInputStream(f.getFile()));
                byte[] buf = new byte[1024];
                int anz;
                while ((anz = bi.read(buf)) != -1) {
                    jo.write(buf, 0, anz);
                }
                bi.close();
            }
            else if (e instanceof BytesEntry) {
                BytesEntry b = (BytesEntry) e;
                jo.write(b.getBytes(), 0, b.getBytes().length);
            }
            contents.add(key);
        }

        jo.close();
        bo.close();
    }
}
