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
package com.wrmsr.presto.wrapper.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Serialization;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class JarSync
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = FileEntry.class, name = "file"),
            @JsonSubTypes.Type(value = DirectoryEntry.class, name = "directory"),
    })
    public static abstract class Entry
    {
        private final String name;
        private final long time;

        public Entry(String name, long time)
        {
            this.name = name;
            this.time = time;
        }

        public Entry(ZipFile zipFile, ZipEntry zipEntry)
        {
            name = zipEntry.getName();
            time = zipEntry.getTime();
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public long getTime()
        {
            return time;
        }

        public static Entry create(ZipFile zipFile, ZipEntry zipEntry)
        {
            if (zipEntry.isDirectory()) {
                return new DirectoryEntry(zipFile, zipEntry);
            }
            else {
                return new FileEntry(zipFile, zipEntry);
            }
        }
    }

    public static final class DirectoryEntry
            extends Entry
    {
        @JsonCreator
        public DirectoryEntry(
                @JsonProperty("name") String name,
                @JsonProperty("time") long time)
        {
            super(name, time);
        }

        public DirectoryEntry(ZipFile zipFile, ZipEntry zipEntry)
        {
            super(zipFile, zipEntry);
        }
    }

    public static String hexForBytes(byte[] bytes)
    {
        StringBuffer sb = new StringBuffer();
        for (byte b : bytes) {
            String h = Integer.toHexString((int) b & 0xff);
            if (h.length() < 2) {
                sb.append("0");
            }
            sb.append(h);
        }
        return sb.toString();
    }

    public static final class FileEntry
            extends Entry
    {
        private final String digest;

        @JsonCreator
        public FileEntry(
                @JsonProperty("name") String name,
                @JsonProperty("time") long time,
                @JsonProperty("digest") String digest)
        {
            super(name, time);
            this.digest = digest;
        }

        public FileEntry(ZipFile zipFile, ZipEntry zipEntry)
        {
            super(zipFile, zipEntry);
            digest = generateDigest(zipFile, zipEntry);
        }

        public static final String DIGEST_ALG = "MD5";

        public static String generateDigest(ZipFile zipFile, ZipEntry zipEntry)
        {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance(DIGEST_ALG);
            }
            catch (NoSuchAlgorithmException e) {
                throw Throwables.propagate(e);
            }
            try {
                try (BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(zipEntry))) {
                    DigestInputStream dis = new DigestInputStream(bis, md);
                    byte[] buffer = new byte[65536];
                    while (dis.read(buffer, 0, buffer.length) > 0) {
                    }
                    return hexForBytes(md.digest());
                }
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @JsonProperty
        public String getDigest()
        {
            return digest;
        }
    }

    public static final class Manifest
    {
        private final String name;
        private final List<Entry> entries;

        @JsonCreator
        public Manifest(
                @JsonProperty("name") String name,
                @JsonProperty("entries") List<Entry> entries)
        {
            this.name = name;
            this.entries = ImmutableList.copyOf(entries);
        }

        public Manifest(ZipFile zipFile)
        {
            name = zipFile.getName();
            ImmutableList.Builder<Entry> builder = ImmutableList.builder();
            Enumeration<? extends ZipEntry> zipEntries;
            for (zipEntries = zipFile.entries(); zipEntries.hasMoreElements(); ) {
                ZipEntry zipEntry = zipEntries.nextElement();
                Entry entry = Entry.create(zipFile, zipEntry);
                builder.add(entry);
            }
            entries = builder.build();
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public List<Entry> getEntries()
        {
            return entries;
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CreateDirectoryOperation.class, name = "createDirectory"),
            @JsonSubTypes.Type(value = CopyFileOperation.class, name = "copyFile"),
            @JsonSubTypes.Type(value = TransferFileOperation.class, name = "transferFile"),
    })
    public static abstract class Operation<E extends Entry>
    {
        private final E entry;

        @JsonCreator
        public Operation(
                @JsonProperty("entry") E entry)
        {
            this.entry = entry;
        }

        @JsonProperty
        public E getEntry()
        {
            return entry;
        }
    }

    public static final class CreateDirectoryOperation extends Operation<DirectoryEntry>
    {
        @JsonCreator
        public CreateDirectoryOperation(
                @JsonProperty("entry") DirectoryEntry entry)
        {
            super(entry);
        }
    }

    public static final class CopyFileOperation extends Operation<FileEntry>
    {
        @JsonCreator
        public CopyFileOperation(
                @JsonProperty("entry") FileEntry entry)
        {
            super(entry);
        }
    }

    public static final class TransferFileOperation extends Operation<FileEntry>
    {
        @JsonCreator
        public TransferFileOperation(
                @JsonProperty("entry") FileEntry entry)
        {
            super(entry);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        ObjectMapper objectMapper = Serialization.JSON_OBJECT_MAPPER.get();
        File jarFile = new File("/Users/spinlock/presto/presto");
        try (ZipFile zipFile = new ZipFile(jarFile)) {
            Manifest manifest = new Manifest(zipFile);
            System.out.println(objectMapper.writeValueAsString(manifest));
        }
    }
}
