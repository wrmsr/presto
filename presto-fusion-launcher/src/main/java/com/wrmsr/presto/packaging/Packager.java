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
package com.wrmsr.presto.packaging;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.wrmsr.presto.packaging.entries.BytesEntry;
import com.wrmsr.presto.packaging.entries.Entry;
import com.wrmsr.presto.packaging.entries.FileEntry;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.artifact.ArtifactType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
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

    private void add(File source, JarOutputStream target)
            throws IOException
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

    public static String shellExec(String... args)
            throws Throwable
    {
        Runtime rt = Runtime.getRuntime();
        Process proc = rt.exec(args);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        String ret = stdInput.readLine();
        proc.waitFor(10, TimeUnit.SECONDS);
        checkState(proc.exitValue() == 0);
        return ret;
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
                "presto-redis",
                "presto-wrmsr-main",
                "presto-wrmsr-hadoop",
                "presto-wrmsr-jython"
                // "presto-wrmsr-jruby",
                // "presto-wrmsr-lucene",
        );
    }

    public void run()
            throws Throwable
    {
        // File cwd = new File(System.getProperty("user.dir"));
        String topLevel = shellExec("git", "rev-parse", "--show-toplevel");
        File cwd = new File(topLevel);

        String head = shellExec("git", "rev-parse", "--verify", "HEAD"); // FIXME append -SNAPSHOT if dirty
        String short_ = shellExec("git", "rev-parse", "--short", "HEAD"); // FIXME append -SNAPSHOT if dirty
        String tags = shellExec("git", "describe", "--tags");

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

        String wrapperProject = "presto-wrmsr-launcher";
        List<String> names = getModuleNames();
        Set<String> localGroups = ImmutableSet.of(
                "com.facebook.presto",
                "com.wrmsr.presto"
        );

        File wrapperJarFile = null;
        Set<Entry> entries = newHashSet();
        Set<String> uncachedRepoPaths = newHashSet();
        for (String name : names) {
            wrapperJarFile = processName(cwd, jarRepoBase, classpathBase, repository, resolver, wrapperProject, localGroups, wrapperJarFile, entries, uncachedRepoPaths, name);
        }

        List<String> sortedUncachedRepoPaths = newArrayList(uncachedRepoPaths);
        Collections.sort(sortedUncachedRepoPaths);
        String uncachedRepoPathsStr = String.join("\n", sortedUncachedRepoPaths) + "\n";
        entries.add(new BytesEntry("classpaths/.uncached", uncachedRepoPathsStr.getBytes(), System.currentTimeMillis()));

        checkState(wrapperJarFile != null);
        Map<String, Entry> entryMap = entries.stream().collect(toMap(Entry::getJarPath, e -> e));
        List<String> keys = newArrayList(entryMap.keySet());
        Collections.sort(keys);
        checkState(keys.size() == newHashSet(keys).size());

        String outPath = System.getProperty("user.home") + "/presto/presto.jar";
        buildJar(head, short_, tags, wrapperJarFile, entryMap, keys, outPath);

        byte[] launcherBytes;
        try (InputStream launcherStream = Packager.class.getClassLoader().getResourceAsStream("com/wrmsr/presto/launcher/launcher")) {
            launcherBytes = CharStreams.toString(new InputStreamReader(launcherStream, Charsets.UTF_8)).getBytes();
        }

        // TODO suffix with git sha
        String exePath = System.getProperty("user.home") + "/presto/presto";
        try (InputStream fi = new BufferedInputStream(new FileInputStream(outPath));
                OutputStream fo = new BufferedOutputStream(new FileOutputStream(exePath))) {
            fo.write(launcherBytes, 0, launcherBytes.length);
            fo.write(new byte[] {'\n', '\n'});
            byte[] buf = new byte[65536];
            int anz;
            while ((anz = fi.read(buf)) != -1) {
                fo.write(buf, 0, anz);
            }
        }
        new File(exePath).setExecutable(true, false);

        new File(outPath).delete();
    }

    private File processName(File cwd, File jarRepoBase, File classpathBase, File repository, ArtifactResolver resolver, String wrapperProject, Set<String> localGroups, File wrapperJarFile, Set<Entry> entries, Set<String> uncachedRepoPaths, String name)
            throws MalformedURLException
    {
        String pom = name + "/pom.xml";
        // log.info(pom);

        List<String> repoPaths = newArrayList();
        File pomFile = new File(cwd, pom);
        List<Artifact> artifacts = resolver.resolvePom(pomFile);
        Map<Boolean, List<Artifact>> p = artifacts.stream().collect(Collectors.partitioningBy(a -> "org.slf4j".equals(a.getGroupId())));
        artifacts = newArrayList(p.getOrDefault(false, ImmutableList.of()));
        if (p.containsKey(true)) {
            List<Artifact> as = p.get(true);
            if (!as.isEmpty()) {
                String v = as.stream().collect(Collectors.maxBy(comparing(Artifact::getVersion))).get().getVersion();
                artifacts.addAll(resolver.resolveArtifacts(as.stream().map(
                        a -> new DefaultArtifact(a.getGroupId(), a.getArtifactId(), a.getClassifier(), a.getExtension(), v, a.getProperties(), (ArtifactType) null)).collect(toImmutableList())));
            }
        }

        List<File> files = newArrayList();
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
        entries.add(new BytesEntry(classpathPath, repoPathsStr.getBytes(), System.currentTimeMillis()));
        return wrapperJarFile;
    }

    private void buildJar(String head, String short_, String tags, File wrapperJarFile, Map<String, Entry> entryMap, List<String> keys, String outPath)
            throws IOException
    {
        BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(outPath));
        JarOutputStream jo = new JarOutputStream(bo);

        Set<String> contents = newHashSet();
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
            String p = e.getJarPath();
            List<String> pathParts = newArrayList(p.split("/"));
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
            e.processEntry(je);
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

        JarEntry jeHEAD = new JarEntry("HEAD");
        jo.putNextEntry(jeHEAD);
        jo.write(head.getBytes());

        JarEntry jeSHORT = new JarEntry("SHORT");
        jo.putNextEntry(jeSHORT);
        jo.write(short_.getBytes());

        JarEntry jeTAGS = new JarEntry("TAGS");
        jo.putNextEntry(jeTAGS);
        jo.write(tags.getBytes());

        jo.close();
        bo.close();
    }
}

