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
package com.wrmsr.presto.launcher.packaging.jarBuilder;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.BytesJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.DirectoryJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.FileJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.JarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.JarBuilderEntryVisitor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.MoreIO.readFullyAndClose;

public final class JarBuilder
{
    private JarBuilder()
    {
    }

    public static final Set<String> SPECIAL_NAMES = ImmutableSet.of(
            "META-INF/MANIFEST.MF"
    );

    @FunctionalInterface
    public interface ZipEntryConsumer
    {
        void accept(ZipFile zipFile, ZipEntry zipEntry)
                throws IOException;
    }

    public static void enumerateZipEntries(File inFile, ZipEntryConsumer consumer)
            throws IOException
    {
        checkArgument(inFile.isFile());
        Set<String> seenNames = new HashSet<>();
        try (ZipFile zipFile = new ZipFile(inFile)) {
            Enumeration<? extends ZipEntry> zipEntries;
            for (zipEntries = zipFile.entries(); zipEntries.hasMoreElements(); ) {
                ZipEntry zipEntry = zipEntries.nextElement();
                checkState(!seenNames.contains(zipEntry.getName()));
                seenNames.add(zipEntry.getName());
                consumer.accept(zipFile, zipEntry);
            }
        }
    }

    public static List<JarBuilderEntry> getEntriesAsBytes(File inFile)
            throws IOException
    {
        List<JarBuilderEntry> entries = new ArrayList<>();
        enumerateZipEntries(inFile, (zipFile, zipEntry) -> {
            if (zipEntry.isDirectory()) {
                entries.add(
                        new DirectoryJarBuilderEntry(
                                zipEntry.getName(),
                                zipEntry.getTime()));
            }
            else {
                byte[] bytes = readFullyAndClose(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize());
                entries.add(
                        new BytesJarBuilderEntry(
                                zipEntry.getName(),
                                zipEntry.getTime(),
                                bytes));
            }
        });
        return entries;
    }

    public static List<JarBuilderEntry> getEntriesAsFiles(File inFile, File outDir)
            throws IOException
    {
        checkArgument(outDir.isDirectory());
        List<JarBuilderEntry> entries = new ArrayList<>();
        enumerateZipEntries(inFile, (zipFile, zipEntry) -> {
            if (zipEntry.isDirectory()) {
                entries.add(
                        new DirectoryJarBuilderEntry(
                                zipEntry.getName(),
                                zipEntry.getTime()));
            }
            else {
                byte[] bytes = readFullyAndClose(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize());
                Path entryPath = new File(outDir, zipEntry.getName()).toPath();
                checkState(!entryPath.toFile().exists());
                checkState(entryPath.toAbsolutePath().startsWith(outDir.toPath()));
                Files.createDirectories(entryPath.getParent());
                Files.write(entryPath, bytes);
                Files.setLastModifiedTime(entryPath, FileTime.from(zipEntry.getTime(), TimeUnit.SECONDS));
                entries.add(
                        new FileJarBuilderEntry(
                                zipEntry.getName(),
                                entryPath.toFile()));
            }
        });
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

    public static void buildJar(List<JarBuilderEntry> entries, File outFile)
            throws IOException
    {
        Set<String> seenNames = new HashSet<>();
        Set<String> seenDirectories = new HashSet<>();
        try (BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outFile));
                JarOutputStream jo = new JarOutputStream(bo)) {
            for (JarBuilderEntry jarBuilderEntry : entries) {
                checkState(!seenNames.contains(jarBuilderEntry.getName()));
                seenNames.add(jarBuilderEntry.getName());

                if (!SPECIAL_NAMES.contains(jarBuilderEntry.getName())) {
                    for (String directory : getNameDirectories(jarBuilderEntry.getName())) {
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
                    }
                }

                jarBuilderEntry.accept(new JarBuilderEntryVisitor<Void, Void>()
                {
                    @Override
                    public Void visitEntry(JarBuilderEntry jarBuilderEntry, Void context)
                    {
                        throw new IllegalStateException();
                    }

                    @Override
                    public Void visitBytesEntry(BytesJarBuilderEntry entry, Void context)
                    {
                        try {
                            JarEntry jarEntry = new JarEntry(entry.getName());
                            entry.bestowJarEntryAttributes(jarEntry);
                            jo.putNextEntry(jarEntry);
                            jo.write(entry.getBytes(), 0, entry.getBytes().length);
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                        return null;
                    }

                    @Override
                    public Void visitDirectoryEntry(DirectoryJarBuilderEntry entry, Void context)
                    {
                        return null;
                    }

                    @Override
                    public Void visitFileEntry(FileJarBuilderEntry entry, Void context)
                    {
                        try {
                            JarEntry jarEntry = new JarEntry(entry.getName());
                            entry.bestowJarEntryAttributes(jarEntry);
                            jo.putNextEntry(jarEntry);
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
