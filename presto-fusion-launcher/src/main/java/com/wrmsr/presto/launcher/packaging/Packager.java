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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.wrmsr.presto.launcher.packaging.entries.BytesEntry;
import com.wrmsr.presto.launcher.packaging.entries.Entry;
import com.wrmsr.presto.launcher.packaging.entries.FileEntry;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.artifact.ArtifactType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toMap;

// FIXME this is a pile of shit.

public class Packager
{
    private static final Logger log = Logger.get(Packager.class);

    private Packager()
    {
    }

    public static void main(String[] args)
            throws Throwable
    {
        Logging.initialize();

        new Packager().run();
    }

    public List<String> getModuleNames()
    {
        return ImmutableList.of(
                "presto-hive-hadoop2",

                "presto-fusion-launcher", // BOOTSTRAP SELF

                "presto-main",
                "presto-blackhole",
                "presto-cli",
                "presto-cassandra",
                "presto-example-http",
                "presto-jmx",
                "presto-kafka",
                "presto-ml",
                "presto-mysql",
                "presto-postgresql",
                "presto-raptor",
                "presto-teradata-functions",
                "presto-local-file",
                "presto-tpch",
                "presto-redis"
        );
    }

    public void run()
            throws Throwable
    {
        GitInfo gitInfo = GitInfo.get();

        // File cwd = new File(System.getProperty("user.dir"));
        File cwd = gitInfo.getTopLevel().toFile();

        for (String logName : new String[] {"com.ning.http.client.providers.netty.NettyAsyncHttpProvider"}) {
            java.util.logging.Logger.getLogger(logName).setLevel(Level.WARNING);
        }

        // FIXME: META-INF/MANIFEST.MF Class-Path
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jar.html
        File jarRepoBase = new File("/repository");
        File classpathBase = new File("/classpaths");
        String m2Home = System.getenv("M2_HOME");
        if (Strings.isNullOrEmpty(m2Home)) {
            m2Home = new File(new File(System.getProperty("user.home")), ".m2").getAbsolutePath();
        }
        File repository = new File(new File(m2Home), "repository");

        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        String wrapperProject = "presto-fusion-launcher";
        Set<String> localGroups = ImmutableSet.of(
                "com.facebook.presto",
                "com.wrmsr.presto"
        );

        File wrapperJarFile = null;
        Set<Entry> entries = new HashSet<>();
        Set<String> uncachedRepoPaths = new HashSet<>();
        for (String name : getModuleNames()) {
            wrapperJarFile = processName(cwd, jarRepoBase, classpathBase, repository, resolver, wrapperProject, localGroups, wrapperJarFile, entries, uncachedRepoPaths, name);
        }

        List<String> sortedUncachedRepoPaths = new ArrayList<>(uncachedRepoPaths);
        Collections.sort(sortedUncachedRepoPaths);
        String uncachedRepoPathsStr = String.join("\n", sortedUncachedRepoPaths) + "\n";
        entries.add(new BytesEntry("classpaths/.uncached", System.currentTimeMillis(), uncachedRepoPathsStr.getBytes()));

        checkState(wrapperJarFile != null);
        Map<String, Entry> entryMap = entries.stream().collect(toMap(Entry::getName, e -> e));
        List<String> keys = new ArrayList<>(entryMap.keySet());
        Collections.sort(keys);
        checkState(keys.size() == new HashSet<>(keys).size());

        String outPath = System.getProperty("user.home") + "/presto/presto.jar";
        buildJar(wrapperJarFile, entryMap, keys, outPath);

        String exePath = System.getProperty("user.home") + "/presto/presto";
        Jars.makeExecutableJar(new File(outPath), new File(exePath));
        new File(outPath).delete();
    }

    private File processName(File cwd, File jarRepoBase, File classpathBase, File repository, ArtifactResolver resolver, String wrapperProject, Set<String> localGroups, File wrapperJarFile, Set<Entry> entries, Set<String> uncachedRepoPaths, String name)
            throws IOException
    {
        Jars.getJarEntries(new File(System.getProperty("user.home") + "/presto/presto"));
        String pom = name + "/pom.xml";

        List<String> repoPaths = new ArrayList<>();
        File pomFile = new File(cwd, pom);
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        Map<Boolean, List<Artifact>> p = artifacts.stream().collect(Collectors.partitioningBy(a -> "org.slf4j".equals(a.getGroupId())));
        artifacts = new ArrayList<>(p.getOrDefault(false, ImmutableList.of()));
        if (p.containsKey(true)) {
            List<Artifact> as = p.get(true);
            if (!as.isEmpty()) {
                String v = as.stream().collect(Collectors.maxBy(comparing(Artifact::getVersion))).get().getVersion();
                artifacts.addAll(resolver.resolveArtifacts(as.stream().map(
                        a -> new DefaultArtifact(a.getGroupId(), a.getArtifactId(), a.getClassifier(), a.getExtension(), v, a.getProperties(), (ArtifactType) null)).collect(toImmutableList())));
            }
        }

        List<File> files = new ArrayList<>();
        for (Artifact a : artifacts) {
            // if (a.getGroupId().equals("org.slf4j") && a.getArtifactId().equals(("slf4j-log4j12"))) {
            //     continue; // FIXME FUCK YOU
            // }
            if (localGroups.contains(a.getGroupId()) && new File(cwd, a.getArtifactId()).exists()) {
                File localPath = new File(cwd, a.getArtifactId());
                File file;
                String rel;
                if (a.getArtifactId().equals(name)) {
                    String jarFileName = a.getArtifactId() + "-" + a.getVersion() + ".jar";
                    file = new File(pomFile.getParentFile(), "target/" + jarFileName);
                    rel = a.getGroupId().replace(".", "/") + "/" + jarFileName;
                }
                else {
                    file = a.getFile();
                    rel = repository.toURI().relativize(file.toURI()).getPath();
                }
                checkState(file.exists());
                File localFile = new File(localPath, "target/" + file.getName());

                checkState(!rel.startsWith("/") && !rel.startsWith(".."));
                //log.info(rel);

                File jarPath = new File(jarRepoBase, rel);
                String jarPathStr = jarPath.toString();
                checkState(jarPathStr.startsWith("/"));
                jarPathStr = jarPathStr.substring(1);

                if (a.getArtifactId().equals(wrapperProject)) {
                    wrapperJarFile = localFile;
                }
                else {
                    entries.add(new FileEntry(jarPathStr, localFile));
                    checkState(jarPathStr.startsWith("repository/"));
                    repoPaths.add(jarPathStr.substring("repository/".length()));
                    uncachedRepoPaths.add(jarPathStr.substring("repository/".length()));
                }
            }
            else {
                files.add(a.getFile());
            }
        }

        for (File file : files) {
            checkState(file.exists(), String.format("File not found: %s", file));
            URL url = new URL("file:" + file.getAbsolutePath() + (file.isDirectory() ? "/" : ""));
            //log.info(url.toString());
            checkState(!file.isDirectory());
            String rel = repository.toURI().relativize(file.toURI()).getPath();
            checkState(!rel.startsWith("/") && !rel.startsWith(".."));
            //log.info(rel);

            File jarPath = new File(jarRepoBase, rel);
            String jarPathStr = jarPath.toString();
            checkState(jarPathStr.startsWith("/"));
            jarPathStr = jarPathStr.substring(1);

            entries.add(new FileEntry(jarPathStr, file));

            //log.info(jarPathStr);
            checkState(jarPathStr.startsWith("repository/"));
            repoPaths.add(jarPathStr.substring("repository/".length()));
        }

        String repoPathsStr = String.join("\n", repoPaths) + "\n";
        String classpathPath = new File(classpathBase, name).toString();
        checkState(classpathPath.startsWith("/"));
        classpathPath = classpathPath.substring(1);
        entries.add(new BytesEntry(classpathPath, System.currentTimeMillis(), repoPathsStr.getBytes()));
        return wrapperJarFile;
    }

    private void buildJar(File wrapperJarFile, Map<String, Entry> entryMap, List<String> keys, String outPath)
            throws IOException
    {
        BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outPath));
        JarOutputStream jo = new JarOutputStream(bo);

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
