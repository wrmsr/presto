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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;


public class JarSync
{

    public static class Entry implements Comparable
    {

        public final String path;
        public final String digest;

        public Entry(String path, String digest)
        {
            this.path = path;
            this.digest = digest;
        }

        @Override
        public String toString()
        {
            return String.format("%s %s", digest, path);
        }

        public static Entry fromString(String str)
        {
            int idx = str.indexOf(" ");
            if (idx < 0) {
                throw new IllegalArgumentException("str");
            }
            return new Entry(str.substring(0, idx), str.substring(idx + 1));
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

            if (digest != null ? !digest.equals(entry.digest) : entry.digest != null) {
                return false;
            }
            if (path != null ? !path.equals(entry.path) : entry.path != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = path != null ? path.hashCode() : 0;
            result = 31 * result + (digest != null ? digest.hashCode() : 0);
            return result;
        }

        @Override
        public int compareTo(Object o)
        {
            if (o == null || o.getClass() != getClass()) {
                throw new IllegalArgumentException("o");
            }
            Entry other = (Entry) o;
            int ret = path.compareTo(other.path);
            if (ret == 0) {
                ret = digest.compareTo(other.digest);
            }
            return ret;
        }
    }

    public static List<ZipEntry> getZipEntries(ZipFile zipFile)
    {
        ArrayList<ZipEntry> zipEntries = new ArrayList<ZipEntry>();
        Enumeration<? extends ZipEntry> entries;
        for (entries = zipFile.entries(); entries.hasMoreElements(); ) {
            zipEntries.add(entries.nextElement());
        }
        return zipEntries;
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

    public static Entry generateEntry(ZipFile zipFile, ZipEntry zipEntry) throws IOException
    {
        byte[] digest;
        BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(zipEntry));
        try {
            MessageDigest md;
            try {
                md = MessageDigest.getInstance(DIGEST_ALG);
            }
            catch (NoSuchAlgorithmException e) {
                System.err.println(e.toString());
                throw new IllegalArgumentException(DIGEST_ALG);
            }
            DigestInputStream dis = new DigestInputStream(bis, md);
            byte[] buffer = new byte[65536];
            while (dis.read(buffer, 0, buffer.length) > 0) {
            }
            digest = md.digest();
        }
        finally {
            bis.close();
        }
        return new Entry(zipEntry.getName(), hexForBytes(digest));
    }

    public static List<Entry> generateJarEntries(final String jarPath, ExecutorService executor)
    {
        List<ZipEntry> zipEntries;
        try {
            ZipFile zipFile = new ZipFile(jarPath);
            zipEntries = getZipEntries(zipFile);
        }
        catch (IOException e) {
            System.err.println(e.toString());
            return null;
        }

        final List<Entry> entries = new ArrayList<Entry>();
        List<ZipFile> zipFiles = new ArrayList<ZipFile>();
        final ThreadLocal<ZipFile> zipFileTL = new ThreadLocal<ZipFile>()
        {
            @Override
            protected ZipFile initialValue()
            {
                try {
                    return new ZipFile(jarPath);
                }
                catch (IOException e) {
                    System.err.println(e.toString());
                    return null;
                }
            }
        };
        try {
            List<Callable<Entry>> callables = new ArrayList<Callable<Entry>>();
            for (final ZipEntry zipEntry : zipEntries) {
                Callable callable = new Callable<Entry>()
                {
                    @Override
                    public Entry call() throws Exception
                    {
                        ZipFile zipFile = zipFileTL.get();
                        return generateEntry(zipFile, zipEntry);
                    }
                };
                callables.add(callable);
            }
            try {
                List<Future<Entry>> entryFutures = executor.invokeAll(callables);
                for (Future<Entry> entryFuture : entryFutures) {
                    entries.add(entryFuture.get());
                }
            }
            catch (Exception e) {
                System.err.println(e.toString());
                return null;
            }
        }
        finally {
            for (ZipFile listZipFile : zipFiles) {
                try {
                    listZipFile.close();
                }
                catch (IOException e) {
                    System.err.println(e.toString());
                }
            }
        }
        return entries;
    }

    public static Map<String, String> createEntryMap(List<Entry> entries)
    {
        Map<String, String> entryMap = new HashMap<String, String>();
        for (Entry entry : entries) {
            entryMap.put(entry.path, entry.digest);
        }
        return entryMap;
    }

    public static List<Operation> diffEntryMaps(Map<String, String> from, Map<String, String> to)
    {
        List<Operation> ops = new ArrayList<Operation>();
        for (Map.Entry<String, String> fromEntry : from.entrySet()) {
            if (!to.containsKey(fromEntry.getKey())) {
                ops.add(new Operation.Delete(fromEntry.getKey()));
            }
            else {
                String toDigest = to.get(fromEntry.getKey());
                if (!toDigest.equals(fromEntry.getValue())) {
                    ops.add(new Operation.Update(fromEntry.getKey()));
                }
            }
        }
        for (Map.Entry<String, String> toEntry : to.entrySet()) {
            if (!from.containsKey(toEntry.getKey())) {
                ops.add(new Operation.Insert(toEntry.getKey()));
            }
        }
        return ops;
    }

    public static class IO
    {

        public static final int BLOCK_SIZE = 65536;

        public static void move(InputStream in, OutputStream out, int len) throws IOException
        {
            byte[] buf = new byte[BLOCK_SIZE];
            int pos = 0;
            while (pos < len) {
                int i = (pos + BLOCK_SIZE) > len ? (len - pos) : BLOCK_SIZE;
                int s = in.read(buf, 0, i);
                if (s < 0) {
                    break;
                }
                out.write(buf, 0, s);
                pos += s;
            }
            if (pos != len) {
                throw new IOException();
            }
        }

        public static byte[] readBytes(InputStream in, int len) throws IOException
        {
            byte[] buf = new byte[len];
            int pos = 0;
            while (pos < len) {
                int s = in.read(buf, pos, len - pos);
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

        public static int readInt(InputStream in) throws IOException
        {
            return ByteBuffer.wrap(readBytes(in, 4)).getInt();
        }

        public static byte[] readBytes(InputStream in) throws IOException
        {
            int len = readInt(in);
            return readBytes(in, len);
        }

        public static String readString(InputStream in) throws IOException
        {
            return new String(readBytes(in));
        }

        public static void readStream(InputStream in, OutputStream out) throws IOException
        {
            int len = readInt(in);
            move(in, out, len);
        }

        public static void writeInt(OutputStream out, int i) throws IOException
        {
            byte[] buf = new byte[4];
            ByteBuffer.wrap(buf).putInt(i);
            out.write(buf);
        }

        public static void writeBytes(OutputStream out, byte[] buf) throws IOException
        {
            writeInt(out, buf.length);
            out.write(buf);
        }

        public static void writeString(OutputStream out, String s) throws IOException
        {
            writeBytes(out, s.getBytes());
        }

        public static void writeStream(OutputStream out, InputStream in, int len) throws IOException
        {
            writeInt(out, len);
            move(in, out, len);
        }
    }

    public static abstract class Operation
    {

        public final String name;

        protected Operation(String name)
        {
            this.name = name;
        }

        public void write(ZipFile zipFile, OutputStream out) throws IOException
        {
            IO.writeString(out, getClass().getSimpleName());
            IO.writeString(out, name);
            unapply(zipFile, out);
        }

        public abstract void unapply(ZipFile zipFile, OutputStream out) throws IOException;

        public static class ReadContext
        {

            public final ZipFile srcFile;
            public final ZipOutputStream dstFile;
            public final Set<String> filtered = new HashSet<String>();

            public ReadContext(ZipFile srcFile, ZipOutputStream dstFile)
            {
                this.srcFile = srcFile;
                this.dstFile = dstFile;
            }
        }

        public static void read(InputStream in, ReadContext out) throws IOException
        {
            String className = IO.readString(in);
            String name = IO.readString(in);
            Operation op;
            if (Insert.class.getSimpleName().equals(className)) {
                op = new Insert(name);
            }
            else if (Update.class.getSimpleName().equals(className)) {
                op = new Update(name);
            }
            else if (Delete.class.getSimpleName().equals(className)) {
                op = new Delete(name);
            }
            else {
                throw new IllegalArgumentException("className");
            }
            op.apply(in, out);
        }

        public abstract void apply(InputStream in, ReadContext out) throws IOException;

        @Override
        public String toString()
        {
            return String.format("%s(%s)", getClass().getSimpleName(), name);
        }

        public static void writeAll(ZipFile zipFile, OutputStream out, List<Operation> ops) throws IOException
        {
            IO.writeInt(out, ops.size());
            for (Operation op : ops) {
                op.write(zipFile, out);
            }
        }

        public static void readAll(InputStream in, ZipFile srcFile, OutputStream out) throws IOException
        {
            int count = IO.readInt(in);
            ZipOutputStream dstFile = new ZipOutputStream(out);
            try {
                ReadContext rc = new ReadContext(srcFile, dstFile);
                for (int i = 0; i < count; i++) {
                    read(in, rc);
                }
                for (ZipEntry ze : getZipEntries(srcFile)) {
                    if (!rc.filtered.contains(ze.getName())) {
                        dstFile.putNextEntry(ze);
                        InputStream zeIn = srcFile.getInputStream(ze);
                        try {
                            IO.move(zeIn, dstFile, (int) ze.getSize());
                        }
                        finally {
                            zeIn.close();
                        }
                    }
                }
            }
            finally {
                dstFile.close();
            }
        }

        public static abstract class PayloadOperation extends Operation
        {

            protected PayloadOperation(String name)
            {
                super(name);
            }

            @Override
            public void unapply(ZipFile zipFile, OutputStream out) throws IOException
            {
                ZipEntry entry = zipFile.getEntry(name);
                if (entry == null) {
                    throw new IllegalArgumentException("name");
                }
                InputStream in = zipFile.getInputStream(entry);
                try {
                    int len = (int) entry.getSize();
                    IO.writeInt(out, len);
                    IO.move(in, out, len);
                }
                finally {
                    in.close();
                }
            }

            @Override
            public void apply(InputStream in, ReadContext out) throws IOException
            {
                out.filtered.add(name);
                int len = IO.readInt(in);
                ZipEntry e = new ZipEntry(name);
                out.dstFile.putNextEntry(e);
                IO.move(in, out.dstFile, len);
            }
        }

        public static class Insert extends PayloadOperation
        {

            public Insert(String name)
            {
                super(name);
            }
        }

        public static class Update extends PayloadOperation
        {

            public Update(String name)
            {
                super(name);
            }
        }

        public static class Delete extends Operation
        {

            public Delete(String name)
            {
                super(name);
            }

            @Override
            public void unapply(ZipFile zipFile, OutputStream out) throws IOException
            {
                if (zipFile.getEntry(name) != null) {
                    throw new IllegalArgumentException("name");
                }
            }

            @Override
            public void apply(InputStream in, ReadContext out) throws IOException
            {
                if (out.srcFile.getEntry(name) == null) {
                    throw new IllegalArgumentException("name");
                }
                out.filtered.add(name);
            }
        }

    }

    public static void main(String[] args) throws Exception
    {
        int nThreads = 8;
        String fromJarPath = "/Users/wtimoney/morgoth-old.jar";
        String toJarPath = "/Users/wtimoney/morgoth-new.jar";
        String resultPath = "/Users/wtimoney/morgoth-syncd.jar";

        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        try {
            Map<String, String> from = createEntryMap(generateJarEntries(fromJarPath, executor));
            Map<String, String> to = createEntryMap(generateJarEntries(toJarPath, executor));

            String syncFile = "/Users/wtimoney/morgoth-old-new-diff.bin";

            ZipFile zipFile = new ZipFile(toJarPath);
            try {
                FileOutputStream out = new FileOutputStream(syncFile);
                try {
                    GZIPOutputStream gzOut = new GZIPOutputStream(out);
                    try {
                        Operation.writeAll(zipFile, gzOut, diffEntryMaps(from, to));
                    }
                    finally {
                        gzOut.close();
                    }
                }
                finally {
                    out.close();
                }
            }
            finally {
                zipFile.close();
            }

            zipFile = new ZipFile(fromJarPath);
            InputStream syncIn = new FileInputStream(syncFile);
            try {
                GZIPInputStream syncGzIn = new GZIPInputStream(syncIn);
                try {
                    FileOutputStream resultOut = new FileOutputStream(resultPath);
                    try {
                        Operation.readAll(syncGzIn, zipFile, resultOut);
                    }
                    finally {
                        resultOut.close();
                    }
                }
                finally {
                    syncGzIn.close();
                }
            }
            finally {
                syncIn.close();
            }
        }
        finally {
            executor.shutdown();
        }
    }
}
