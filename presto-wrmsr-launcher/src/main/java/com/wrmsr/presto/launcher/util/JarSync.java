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
package com.wrmsr.presto.launcher.util;

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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.wrmsr.presto.util.ImmutableCollectors.toImmutableMap;

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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry entry = (Entry) o;
            return Objects.equals(time, entry.time) &&
                    Objects.equals(name, entry.name);
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

        public abstract Iterable<Operation> plan(Entry other);
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DirectoryEntry directoryEntry = (DirectoryEntry) o;
            return super.equals(o);
        }

        @Override
        public Iterable<Operation> plan(Entry other)
        {
            return ImmutableList.of(new CreateDirectoryOperation(this));
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
        private final long size;

        @JsonCreator
        public FileEntry(
                @JsonProperty("name") String name,
                @JsonProperty("time") long time,
                @JsonProperty("size") long size,
                @JsonProperty("digest") String digest)
        {
            super(name, time);
            this.size = size;
            this.digest = digest;
        }

        public FileEntry(ZipFile zipFile, ZipEntry zipEntry)
        {
            super(zipFile, zipEntry);
            this.size = zipEntry.getCompressedSize();
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FileEntry fileEntry = (FileEntry) o;
            return super.equals(o) &&
                    Objects.equals(size, fileEntry.size) &&
                    Objects.equals(digest, fileEntry.digest);
        }

        public boolean equalsExceptTime(FileEntry other)
        {
            return getClass() == other.getClass() &&
                    Objects.equals(getName(), other.getName()) &&
                    Objects.equals(size, other.size) &&
                    Objects.equals(digest, other.digest);
        }

        @JsonProperty
        public long getSize()
        {
            return size;
        }

        @JsonProperty
        public String getDigest()
        {
            return digest;
        }

        @Override
        public Iterable<Operation> plan(Entry other)
        {
            if (equals(other)) {
                return ImmutableList.of(new CopyFileOperation(this));
            }
            else if (other instanceof FileEntry && equalsExceptTime((FileEntry) other)) {
                return ImmutableList.of(
                        new CopyFileOperation(this),
                        new SetTimeOperation(getName(), getTime()));
            }
            else {
                return ImmutableList.of(new TransferFileOperation(this));
            }
        }
    }

    public static final class Manifest
            implements Iterable<Entry>
    {
        private final String name;
        private final boolean isExecutable;
        private final byte[] preamble;
        private final List<Entry> entries;

        @JsonCreator
        public Manifest(
                @JsonProperty("name") String name,
                @JsonProperty("isExecutable") boolean isExecutable,
                @JsonProperty("preamble") byte[] preamble,
                @JsonProperty("entries") List<Entry> entries)
        {
            this.name = name;
            this.isExecutable = isExecutable;
            this.preamble = preamble;
            this.entries = ImmutableList.copyOf(entries);
        }

        public Manifest(File file)
                throws IOException
        {
            name = file.getName();
            isExecutable = file.canExecute();
            try (ZipFile zipFile = new ZipFile(file)) {
                long preambleLength = ZipFiles.getPreambleLength(file);
                if (preambleLength > 0) {
                    byte[] preamble = new byte[(int) preambleLength];
                    try (InputStream is = new FileInputStream(file)) {
                        if (is.read(preamble) != preambleLength) {
                            throw new IOException("Failed to read preamble");
                        }
                    }
                    this.preamble = preamble;
                }
                else {
                    this.preamble = null;
                }
                ImmutableList.Builder<Entry> builder = ImmutableList.builder();
                Enumeration<? extends ZipEntry> zipEntries;
                for (zipEntries = zipFile.entries(); zipEntries.hasMoreElements(); ) {
                    ZipEntry zipEntry = zipEntries.nextElement();
                    Entry entry = Entry.create(zipFile, zipEntry);
                    builder.add(entry);
                }
                entries = builder.build();
            }
        }

        @Override
        public Iterator<Entry> iterator()
        {
            return entries.iterator();
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public boolean isExecutable()
        {
            return isExecutable;
        }

        @JsonProperty
        public byte[] getPreamble()
        {
            return preamble;
        }

        @JsonProperty
        public List<Entry> getEntries()
        {
            return entries;
        }

        public Map<String, Entry> getEntryMap()
        {
            return entries.stream().collect(toImmutableMap(Entry::getName, e -> e));
        }

        public Plan plan(Manifest other)
        {
            Map<String, Entry> otherEntries = other.getEntryMap();
            ImmutableList.Builder<Operation> builder = ImmutableList.builder();
            for (Entry entry : this) {
                Entry otherEntry = otherEntries.get(entry.getName());
                builder.addAll(entry.plan(otherEntry));
            }
            if (preamble != null && preamble.length > 0) {
                builder.add(new WritePreambleOperation(preamble));
            }
            if (isExecutable) {
                builder.add(new SetExecutableOperation(true));
            }
            return new Plan(builder.build());
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = WritePreambleOperation.class, name = "writePreamble"),
            @JsonSubTypes.Type(value = SetExecutableOperation.class, name = "setExecutable"),
            @JsonSubTypes.Type(value = CreateDirectoryOperation.class, name = "createDirectory"),
            @JsonSubTypes.Type(value = CopyFileOperation.class, name = "copyFile"),
            @JsonSubTypes.Type(value = SetTimeOperation.class, name = "setTime"),
            @JsonSubTypes.Type(value = TransferFileOperation.class, name = "transferFile"),
    })
    public static abstract class Operation<T>
    {
        private final T subject;

        @JsonCreator
        public Operation(
                @JsonProperty("subject") T subject)
        {
            this.subject = subject;
        }

        @JsonProperty
        public T getSubject()
        {
            return subject;
        }
    }

    public static final class WritePreambleOperation
            extends Operation<byte[]>
    {
        @JsonCreator
        public WritePreambleOperation(
                @JsonProperty("subject") byte[] subject)
        {
            super(subject);
        }
    }

    private static final class SetExecutableOperation
            extends Operation<Boolean>
    {
        @JsonCreator
        public SetExecutableOperation(
                @JsonProperty("subject") Boolean subject)
        {
            super(subject);
        }
    }

    public static final class CreateDirectoryOperation
            extends Operation<DirectoryEntry>
    {
        @JsonCreator
        public CreateDirectoryOperation(
                @JsonProperty("subject") DirectoryEntry subject)
        {
            super(subject);
        }
    }

    public static final class CopyFileOperation
            extends Operation<FileEntry>
    {
        @JsonCreator
        public CopyFileOperation(
                @JsonProperty("subject") FileEntry subject)
        {
            super(subject);
        }
    }

    public static final class TransferFileOperation
            extends Operation<FileEntry>
    {
        @JsonCreator
        public TransferFileOperation(
                @JsonProperty("subject") FileEntry subject)
        {
            super(subject);
        }
    }

    public static final class SetTimeOperation
            extends Operation<String>
    {
        private final long time;

        @JsonCreator
        public SetTimeOperation(
                @JsonProperty("subject") String subject,
                @JsonProperty("time") long time)
        {
            super(subject);
            this.time = time;
        }

        @JsonProperty
        public long getTime()
        {
            return time;
        }
    }

    public static class Plan
            implements Iterable<Operation>
    {
        private final List<Operation> operations;

        @JsonCreator
        public Plan(
                @JsonProperty("operations") List<Operation> operations)
        {
            this.operations = operations;
        }

        @JsonProperty
        public List<Operation> getOperations()
        {
            return operations;
        }

        @Override
        public Iterator<Operation> iterator()
        {
            return operations.iterator();
        }
    }

    public static class InputChannel
    {
        private final InputStream stream;

        public InputChannel(InputStream stream)
        {
            this.stream = stream;
        }

        protected byte[] readBytes(int len)
                throws IOException
        {
            byte[] buf = new byte[len];
            int pos = 0;
            while (pos < len) {
                int s = stream.read(buf, pos, len - pos);
                if (s < 0) {
                    break;
                }
                pos += s;
            }
            if (pos != len) {
                throw new IOException();
            }
            return buf;
        }

        protected int readInt()
                throws IOException
        {
            return ByteBuffer.wrap(readBytes(4)).getInt();
        }

        protected byte[] readBytes()
                throws IOException
        {
            int len = readInt();
            return readBytes(len);
        }

        protected String readString()
                throws IOException
        {
            return new String(readBytes());

    }

    public static class OutputChannel
    {
        private final OutputStream stream;

        public OutputChannel(OutputStream stream)
        {
            this.stream = stream;
        }

        protected void writeInt(int i)
                throws IOException
        {
            byte[] buf = new byte[4];
            ByteBuffer.wrap(buf).putInt(i);
            stream.write(buf);
        }

        protected void writeBytes(byte[] buf)
                throws IOException
        {
            writeInt(buf.length);
            stream.write(buf);
        }

        protected void writeString(String s)
                throws IOException
        {
            writeBytes(s.getBytes());
        }
    }

    public static abstract class Driver
    {
        public abstract void run(InputChannel input, OutputChannel output)
                throws IOException;
    }

    public static class SourceDriver
            extends Driver
    {
        protected final File sourceFile;

        public SourceDriver(InputStream input, OutputStream output, File sourceFile)
        {
            super(input, output);
            this.sourceFile = sourceFile;
        }

        public File getSourceFile()
        {
            return sourceFile;
        }

        @Override
        public void run()
                throws IOException
        {

        }
    }

    public static class SinkDriver
            extends Driver
    {
        private final File sinkFile;
        private final File outputFile;

        public SinkDriver(InputStream input, OutputStream output, File sinkFile, File outputFile)
        {
            super(input, output);
            this.sinkFile = sinkFile;
            this.outputFile = outputFile;
        }

        public File getSinkFile()
        {
            return sinkFile;
        }

        public File getOutputFile()
        {
            return outputFile;
        }

        @Override
        public void run()
                throws IOException
        {

        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // https://docs.oracle.com/javase/7/docs/api/java/io/PipedOutputStream.html
        ObjectMapper objectMapper = Serialization.JSON_OBJECT_MAPPER.get();

        Manifest sourceManifest = new Manifest(new File(System.getProperty("user.home") + "/presto/presto"));
        Manifest sinkManifest = new Manifest(new File(System.getProperty("user.home") + "/presto/foo.jar"));
        Plan plan = sourceManifest.plan(sinkManifest);

        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(plan));
    }
}
