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
package com.wrmsr.presto.wrapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.resolver.ArtifactResolver;
import org.sonatype.aether.artifact.Artifact;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

public class PrestoWrapperBuilder
{
    private static final Logger log = Logger.get(PrestoWrapperBuilder.class);

    private PrestoWrapperBuilder()
    {
    }

    public static void main(String[] args)
            throws Throwable
    {
        new PrestoWrapperBuilder().run(args);
    }

    public static abstract class Entry
    {
        private final String jarPath;

        public Entry(String jarPath)
        {
            this.jarPath = jarPath;
        }

        public String getJarPath()
        {
            return jarPath;
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

            return !(jarPath != null ? !jarPath.equals(entry.jarPath) : entry.jarPath != null);
        }

        @Override
        public int hashCode()
        {
            return jarPath != null ? jarPath.hashCode() : 0;
        }
    }

    public static class FileEntry extends Entry
    {
        private final File file;

        public FileEntry(String jarPath, File file)
        {
            super(jarPath);
            this.file = file;
        }

        public File getFile()
        {
            return file;
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
            if (!super.equals(o)) {
                return false;
            }

            FileEntry fileEntry = (FileEntry) o;

            return !(file != null ? !file.equals(fileEntry.file) : fileEntry.file != null);
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (file != null ? file.hashCode() : 0);
            return result;
        }
    }

    public static class BytesEntry extends Entry
    {
        public final byte[] bytes;

        public BytesEntry(String jarPath, byte[] bytes)
        {
            super(jarPath);
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
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
            if (!super.equals(o)) {
                return false;
            }

            BytesEntry that = (BytesEntry) o;

            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + (bytes != null ? Arrays.hashCode(bytes) : 0);
            return result;
        }
    }

    public void build() throws IOException
    {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream target = new JarOutputStream(new FileOutputStream("output.jar"), manifest)) {
            add(new File("inputDirectory"), target);
        }
    }

    private void add(File source, JarOutputStream target) throws IOException
    {
        if (source.isDirectory()) {
            String name = source.getPath().replace("\\", "/");
            if (!name.isEmpty()) {
                if (!name.endsWith("/")) {
                    name += "/";
                }
                JarEntry entry = new JarEntry(name);
                entry.setTime(source.lastModified());
                target.putNextEntry(entry);
                target.closeEntry();
            }
            for (File nestedFile : source.listFiles()) {
                add(nestedFile, target);
            }
            return;
        }

        JarEntry entry = new JarEntry(source.getPath().replace("\\", "/"));
        entry.setTime(source.lastModified());
        target.putNextEntry(entry);
        try (InputStream in = new BufferedInputStream(new FileInputStream(source))) {
            byte[] buffer = new byte[1024];
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }
                target.write(buffer, 0, count);
            }
            target.closeEntry();
        }
    }

    public void run(String[] args)
            throws Throwable
    {
        Logging.initialize();

        File jarRepoBase = new File("/repository");
        File classpathBase = new File("/classpaths");
        File repository = new File(System.getProperty("user.home"), ".m2/repository");
        File cwd = new File(System.getProperty("user.dir"));

        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        List<String> names = ImmutableList.of(
                "presto-main",
                "presto-cassandra",
                "presto-blackhole",
                "presto-example-http",
                "presto-hive-hadoop2",
                "presto-kafka",
                "presto-ml",
                "presto-mysql",
                "presto-postgresql",
                "presto-raptor",
                "presto-tpch",
                "presto-wrmsr-extensions",
                "presto-wrmsr-wrapper" // BOOTSTRAP SELF
        );

        Set<Entry> entries = newHashSet();
        for (String name : names) {
            String pom = name + "/pom.xml";
            // log.info(pom);

            List<String> repoPaths = newArrayList();
            File pomFile = new File(cwd, pom);
            List<Artifact> artifacts = resolver.resolvePom(pomFile);

            List<File> files = newArrayList();
            for (Artifact a : artifacts) {
                if (a.getGroupId().equals("com.facebook.presto") && new File(cwd, a.getArtifactId()).exists()) {
                    if (a.getArtifactId().equals(name)) {
                        continue;
                    }
                    File localPath = new File(cwd, a.getArtifactId());
                    File localFile = new File(localPath, "target/" + a.getFile().getName());
                    files.add(localFile);
                }
                else {
                    files.add(a.getFile());
                }
            }

            for (File file : files) {
                checkState(file.exists(), String.format("File not found: %s", file));
                URL url = new URL("file:" + file.getAbsolutePath() + (file.isDirectory() ? "/" : ""));
                //log.info(url.toString());
                if (file.isDirectory()) {
                    // FIXME translate target/classes to jar
                    log.info(file.toString());
                }
                else {
                    String rel = repository.toURI().relativize(file.toURI()).getPath();
                    checkState(!rel.startsWith("/") && !rel.startsWith(".."));
                    //log.info(rel);

                    File jarPath = new File(jarRepoBase, rel);
                    String jarPathStr = jarPath.toString();
                    checkState(jarPathStr.startsWith("/"));
                    jarPathStr = jarPathStr.substring(1);

                    entries.add(new FileEntry(jarPathStr, file));

                    //log.info(jarPathStr);
                    repoPaths.add(jarPathStr);
                }
            }

            String repoPathsStr = String.join("\n", repoPaths) + "\n";
            String classpathPath = new File(classpathBase, name).toString();
            checkState(classpathPath.startsWith("/"));
            classpathPath = classpathPath.substring(1);
            entries.add(new BytesEntry(classpathPath, repoPathsStr.getBytes()));
        }

        System.out.println(entries);
    }
}

