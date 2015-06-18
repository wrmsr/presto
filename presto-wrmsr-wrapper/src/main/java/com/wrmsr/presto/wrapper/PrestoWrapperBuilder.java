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
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

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

        File cwd = new File(System.getProperty("user.dir"));

        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        List<String> poms = ImmutableList.of(
                "presto-main/pom.xml",
                "presto-cassandra/pom.xml",
                "presto-example-http/pom.xml",
                "presto-hive-hadoop2/pom.xml",
                "presto-kafka/pom.xml",
                "presto-ml/pom.xml",
                "presto-mysql/pom.xml",
                "presto-postgresql/pom.xml",
                "presto-raptor/pom.xml",
                "presto-tpch/pom.xml",
                "presto-wrmsr-extensions/pom.xml"
        );

        for (String pom : poms) {
            log.info(pom);

            List<Artifact> artifacts = resolver.resolvePom(new File(cwd, pom));

            List<File> files = Lists.newArrayList();
            for (Artifact a : artifacts) {
                files.add(a.getFile());
            }

            for (File file : files) {
                if (!file.exists()) {
                    // throw new IllegalStateException(file.getAbsolutePath());
                    log.warn(String.format("File not found: %s", file));
                }
                URL url = new URL("file:" + file.getAbsolutePath() + (file.isDirectory() ? "/" : ""));
                log.info(url.toString());
            }
        }
    }
}

