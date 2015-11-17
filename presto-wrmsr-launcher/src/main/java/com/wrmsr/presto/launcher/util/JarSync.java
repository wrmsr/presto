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
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.Exceptions.runtimeThrowing;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public class JarSync
{
    private static final Logger log = Logger.get(JarSync.class);

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

    public static final String DIGEST_ALG = "MD5";

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
            this.size = zipEntry.getSize();
            digest = generateDigest(zipFile, zipEntry);
        }

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
            if (other instanceof FileEntry && equalsExceptTime((FileEntry) other)) {
                return ImmutableList.of(new CopyFileOperation(this));
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
                builder.add(new SetExecutableOperation());
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
            @JsonSubTypes.Type(value = TransferFileOperation.class, name = "transferFile"),
    })
    public static abstract class Operation
    {
    }

    public static final class WritePreambleOperation
            extends Operation
    {
        private final byte[] preamble;

        @JsonCreator
        public WritePreambleOperation(
                @JsonProperty("preamble") byte[] preamble)
        {
            this.preamble = preamble;
        }

        @JsonProperty
        public byte[] getPreamble()
        {
            return preamble;
        }
    }

    public static final class SetExecutableOperation
            extends Operation
    {
        @JsonCreator
        public SetExecutableOperation()
        {
        }
    }

    public static final class CreateDirectoryOperation
            extends Operation
    {
        private final DirectoryEntry entry;

        @JsonCreator
        public CreateDirectoryOperation(
                @JsonProperty("entry") DirectoryEntry entry)
        {
            this.entry = entry;
        }

        @JsonProperty
        public DirectoryEntry getEntry()
        {
            return entry;
        }
    }

    public static final class CopyFileOperation
            extends Operation
    {
        private final FileEntry entry;

        @JsonCreator
        public CopyFileOperation(
                @JsonProperty("entry") FileEntry entry)
        {
            this.entry = entry;
        }

        @JsonProperty
        public FileEntry getEntry()
        {
            return entry;
        }
    }

    public static final class TransferFileOperation
            extends Operation
    {
        private final FileEntry entry;

        @JsonCreator
        public TransferFileOperation(
                @JsonProperty("entry") FileEntry entry)
        {
            this.entry = entry;
        }

        @JsonProperty
        public FileEntry getEntry()
        {
            return entry;
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
        public final InputStream stream;

        public InputChannel(InputStream stream)
        {
            this.stream = stream;
        }

        public byte[] readBytes(int len)
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

        public int readInt()
                throws IOException
        {
            return ByteBuffer.wrap(readBytes(4)).getInt();
        }

        public long readLong()
                throws IOException
        {
            return ByteBuffer.wrap(readBytes(8)).getLong();
        }

        public byte[] readBytes()
                throws IOException
        {
            int len = readInt();
            return readBytes(len);
        }

        public String readString()
                throws IOException
        {
            return new String(readBytes());
        }
    }

    public static class OutputChannel
    {
        public final OutputStream stream;

        public OutputChannel(OutputStream stream)
        {
            this.stream = stream;
        }

        public void writeInt(int i)
                throws IOException
        {
            byte[] buf = new byte[4];
            ByteBuffer.wrap(buf).putInt(i);
            stream.write(buf);
        }

        public void writeLong(long i)
                throws IOException
        {
            byte[] buf = new byte[8];
            ByteBuffer.wrap(buf).putLong(i);
            stream.write(buf);
        }

        public void writeBytes(byte[] buf)
                throws IOException
        {
            writeInt(buf.length);
            stream.write(buf);
        }

        public void writeString(String s)
                throws IOException
        {
            writeBytes(s.getBytes());
        }
    }

    public static abstract class Driver<Context>
    {
        public static final UUID HANDSHAKE_UUID = UUID.fromString("2aaee760-9887-4bb7-9525-5b160820e6bf");

        public abstract void run(ObjectMapper mapper, InputChannel input, OutputChannel output)
                throws IOException;

        public void run(ObjectMapper mapper, InputStream input, OutputStream output)
                throws IOException
        {
            run(mapper, new InputChannel(input), new OutputChannel(output));
        }

        protected void handshake(InputChannel input, OutputChannel output)
                throws IOException
        {
            output.writeLong(HANDSHAKE_UUID.getLeastSignificantBits());
            output.writeLong(HANDSHAKE_UUID.getMostSignificantBits());
            long leastSigBits = input.readLong();
            long mostSigBits = input.readLong();
            UUID uuid = new UUID(mostSigBits, leastSigBits);
            if (!HANDSHAKE_UUID.equals(uuid)) {
                throw new IOException("handshake failure");
            }
        }

        protected Context execute(Plan plan, Context context)
                throws IOException
        {
            for (Operation operation : plan) {
                context = execute(operation, context);
            }
            return context;
        }

        protected Context execute(Operation operation, Context context)
                throws IOException
        {
            if (operation instanceof WritePreambleOperation) {
                return execute((WritePreambleOperation) operation, context);
            }
            else if (operation instanceof SetExecutableOperation) {
                return execute((SetExecutableOperation) operation, context);
            }
            else if (operation instanceof CreateDirectoryOperation) {
                return execute((CreateDirectoryOperation) operation, context);
            }
            else if (operation instanceof CopyFileOperation) {
                return execute((CopyFileOperation) operation, context);
            }
            else if (operation instanceof TransferFileOperation) {
                return execute((TransferFileOperation) operation, context);
            }
            else {
                throw new IllegalStateException();
            }
        }

        protected Context execute(WritePreambleOperation operation, Context context)
                throws IOException
        {
            return context;
        }

        protected Context execute(SetExecutableOperation operation, Context context)
                throws IOException
        {
            return context;
        }

        protected Context execute(CreateDirectoryOperation operation, Context context)
                throws IOException
        {
            return context;
        }

        protected Context execute(CopyFileOperation operation, Context context)
                throws IOException
        {
            return context;
        }

        protected Context execute(TransferFileOperation operation, Context context)
                throws IOException
        {
            return context;
        }
    }

    public static class SourceDriver
            extends Driver<SourceDriver.Context>
    {
        protected class Context
        {
            public final InputChannel input;
            public final OutputChannel output;
            public final ZipFile sourceZipFile;

            public Context(InputChannel input, OutputChannel output, ZipFile sourceZipFile)
            {
                this.input = input;
                this.output = output;
                this.sourceZipFile = sourceZipFile;
            }
        }

        protected final File sourceFile;

        public SourceDriver(File sourceFile)
        {
            this.sourceFile = sourceFile;
        }

        public File getSourceFile()
        {
            return sourceFile;
        }

        @Override
        public void run(ObjectMapper mapper, InputChannel input, OutputChannel output)
                throws IOException
        {
            handshake(input, output);
            Manifest manifest = new Manifest(sourceFile);
            String sinkManifestJson = input.readString();
            Manifest sinkManifest = mapper.readValue(sinkManifestJson, Manifest.class);
            Plan plan = manifest.plan(sinkManifest);
            String planJson = mapper.writeValueAsString(plan);
            output.writeString(planJson);
            try (ZipFile sourceZipFile = new ZipFile(sourceFile)) {
                Context context = new Context(input, output, sourceZipFile);
                context = execute(plan, context);
            }
            handshake(input, output);
        }

        @Override
        protected Context execute(TransferFileOperation operation, Context context)
                throws IOException
        {
            log.info(String.format("Source sending %d bytes for file %s", operation.getEntry().getSize(), operation.getEntry().getName()));
            ZipEntry zipEntry = context.sourceZipFile.getEntry(operation.getEntry().getName());
            long total = 0;
            try (InputStream input = context.sourceZipFile.getInputStream(zipEntry)) {
                byte[] buf = new byte[65536];
                int bc;
                while ((bc = input.read(buf)) != -1) {
                    context.output.stream.write(buf, 0, bc);
                    total += bc;
                }
            }
            log.info(String.format("Source sent %d bytes", total));
            return context;
        }
    }

    public static class SinkDriver
            extends Driver<SinkDriver.Context>
    {
        protected class Context
        {
            public final InputChannel input;
            public final OutputChannel output;
            public final ZipFile sinkZipFile;
            public final JarOutputStream jarOutputStream;

            public Context(InputChannel input, OutputChannel output, ZipFile sinkZipFile, JarOutputStream jarOutputStream)
            {
                this.input = input;
                this.output = output;
                this.sinkZipFile = sinkZipFile;
                this.jarOutputStream = jarOutputStream;
            }
        }

        private final File sinkFile;
        private final File outputFile;

        public SinkDriver(File sinkFile, File outputFile)
        {
            this.sinkFile = sinkFile;
            this.outputFile = outputFile;
        }

        @Override
        public void run(ObjectMapper mapper, InputChannel input, OutputChannel output)
                throws IOException
        {
            handshake(input, output);
            Manifest manifest = new Manifest(sinkFile);
            String manifestJson = mapper.writeValueAsString(manifest);
            output.writeString(manifestJson);
            String planJson = input.readString();
            Plan plan = mapper.readValue(planJson, Plan.class);
            try (ZipFile sinkZipFile = new ZipFile(sinkFile)) {
                JarOutputStream jarOutputStream = new JarOutputStream(
                        new BufferedOutputStream(
                                new FileOutputStream(outputFile)));
                Context context = new Context(input, output, sinkZipFile, jarOutputStream);
                context = execute(plan, context);
                if (context.jarOutputStream != null) {
                    context.jarOutputStream.close();
                }
            }
            handshake(input, output);
        }

        @Override
        protected Context execute(CreateDirectoryOperation operation, Context context)
                throws IOException
        {
            JarEntry jarEntry = new JarEntry(operation.getEntry().getName());
            jarEntry.setTime(operation.getEntry().getTime());
            checkNotNull(context.jarOutputStream).putNextEntry(jarEntry);
            context.jarOutputStream.write(new byte[] {}, 0, 0);
            return context;
        }

        @Override
        protected Context execute(CopyFileOperation operation, Context context)
                throws IOException
        {
            JarEntry jarEntry = new JarEntry(operation.getEntry().getName());
            jarEntry.setTime(operation.getEntry().getTime());
            checkNotNull(context.jarOutputStream).putNextEntry(jarEntry);
            ZipEntry zipEntry = context.sinkZipFile.getEntry(operation.getEntry().getName());
            try (InputStream input = context.sinkZipFile.getInputStream(zipEntry)) {
                byte[] buf = new byte[65536];
                int bc;
                while ((bc = input.read(buf)) != -1) {
                    context.jarOutputStream.write(buf, 0, bc);
                }
            }
            return context;
        }

        @Override
        protected Context execute(TransferFileOperation operation, Context context)
                throws IOException
        {
            log.info(String.format("Sink receiving %d bytes for file %s", operation.getEntry().getSize(), operation.getEntry().getName()));
            JarEntry jarEntry = new JarEntry(operation.getEntry().getName());
            jarEntry.setTime(operation.getEntry().getTime());
            checkNotNull(context.jarOutputStream).putNextEntry(jarEntry);
            long rem = operation.getEntry().getSize();
            byte[] buf = new byte[65536];
            int bc;
            MessageDigest md;
            try {
                md = MessageDigest.getInstance(DIGEST_ALG);
            }
            catch (NoSuchAlgorithmException e) {
                throw Throwables.propagate(e);
            }
            long total = 0;
            while (rem > 0 && (bc = context.input.stream.read(buf, 0, (int) (rem > buf.length ? buf.length : rem))) != -1) {
                context.jarOutputStream.write(buf, 0, bc);
                md.update(buf, 0, bc);
                rem -= bc;
                total += bc;
            }
            log.info(String.format("Sink received %d bytes", total));
            String digest = hexForBytes(md.digest());
            if (!digest.equals(operation.getEntry().getDigest())) {
                throw new IOException("digest mismatch");
            }
            return context;
        }

        @Override
        protected Context execute(WritePreambleOperation operation, Context context)
                throws IOException
        {
            checkNotNull(context.jarOutputStream).finish();
            context.jarOutputStream.close();
            File tempDir = Files.createTempDirectory(null).toFile();
            tempDir.deleteOnExit();
            File tempFile = new File(tempDir, outputFile.getName());
            checkState(outputFile.renameTo(tempFile));
            try (InputStream input = new BufferedInputStream(new FileInputStream(tempFile));
                    OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                output.write(operation.getPreamble());
                byte[] buf = new byte[65536];
                int anz;
                while ((anz = input.read(buf)) != -1) {
                    output.write(buf, 0, anz);
                }
            }
            return new Context(
                    context.input,
                    context.output,
                    context.sinkZipFile,
                    null
            );
        }

        @Override
        protected Context execute(SetExecutableOperation operation, Context context)
                throws IOException
        {
            outputFile.setExecutable(true, false);
            return context;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Logging.initialize();

        File sourceFile = new File(System.getProperty("user.home") + "/presto/presto");

        File sinkFile = new File(System.getProperty("user.home") + "/presto/presto-old");
        File outputFile = new File(System.getProperty("user.home") + "/presto/presto-out");

        ObjectMapper mapper = Serialization.JSON_OBJECT_MAPPER.get();

        SourceDriver sourceDriver = new SourceDriver(sourceFile);
        SinkDriver sinkDriver = new SinkDriver(sinkFile, outputFile);

        PipedInputStream sourceInput = new PipedInputStream();
        PipedOutputStream sourceOutput = new PipedOutputStream(sourceInput);

        PipedInputStream sinkInput = new PipedInputStream();
        PipedOutputStream sinkOutput = new PipedOutputStream(sinkInput);

        Thread sourceThread = new Thread(runtimeThrowing(() -> sourceDriver.run(mapper, sinkInput, sourceOutput)));
        sourceThread.start();

        Thread sinkThread = new Thread(runtimeThrowing(() -> sinkDriver.run(mapper, sourceInput, sinkOutput)));
        sinkThread.start();

        sourceThread.join();
        sinkThread.join();

        // TODO watchdog deathpact

        // Plan plan = sourceManifest.plan(sinkManifest);
        // System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(plan));
    }
}
