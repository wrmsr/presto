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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

public final class Repositories
{
    private Repositories()
    {
    }

    // FIXME add static ClassLoader, set in bootstrap via refl

    public static final String REPOSITORY_CACHE_PATH_PROPERTY_KEY = "com.wrmsr.repository.cache-path";
    public static final String REPOSITORY_PATH_PROPERTY_KEY = "com.wrmsr.repository.path";
    public static final String MADE_REPOSITORY_PATH_PROPERTY_KEY = "com.wrmsr.repository.made-path";

    public static final Set<String> PROPERTY_KEYS;

    static {
        Set<String> tmp = new HashSet<>();
        tmp.add(REPOSITORY_CACHE_PATH_PROPERTY_KEY);
        tmp.add(REPOSITORY_PATH_PROPERTY_KEY);
        tmp.add(MADE_REPOSITORY_PATH_PROPERTY_KEY);
        PROPERTY_KEYS = Collections.unmodifiableSet(tmp);
    }

    public static Map<String, String> getProperties()
    {
        Map<String, String> tmp = new HashMap<>();
        for (String key : PROPERTY_KEYS) {
            String value = System.getProperty(key);
            if (value != null && !value.isEmpty()) {
                tmp.put(key, value);
            }
        }
        return Collections.unmodifiableMap(tmp);
    }

    public static void addClasspathUrl(URLClassLoader classLoader, URL url)
            throws IOException
    {
        try {
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, url);
        }
        catch (Throwable t) {
            t.printStackTrace();
            throw new IOException("Error, could not add URL to system classloader");
        }
    }

    public static void addClasspathUrl(ClassLoader classLoader, URL url)
            throws IOException
    {
        addClasspathUrl((URLClassLoader) classLoader, url);
    }

    public static File getOrMakePropertyPath(String key)
            throws IOException
    {
        String repositoryPathString = System.getProperty(key);
        File repositoryPath;
        if (repositoryPathString == null || repositoryPathString.isEmpty()) {
            repositoryPath = Files.createTempDirectory(null).toFile();
            repositoryPath.deleteOnExit(); // FIXME OSX EXECVE SEGFAULT
            System.setProperty(key, repositoryPath.getAbsolutePath());
            System.setProperty(MADE_REPOSITORY_PATH_PROPERTY_KEY, "true");
        }
        else {
            repositoryPath = new File(repositoryPathString);
            if (!repositoryPath.exists()) {
                throw new IOException("Repository path does not exist: " + repositoryPath);
            }
            if (!repositoryPath.isDirectory()) {
                throw new IOException("Repository path is not a directory: " + repositoryPath);
            }
        }
        return repositoryPath;
    }

    public static File getOrMakeRepositoryPath()
            throws IOException
    {
        return getOrMakePropertyPath(REPOSITORY_PATH_PROPERTY_KEY);
    }

    public static String getRepositoryPath()
    {
        return System.getProperty(REPOSITORY_PATH_PROPERTY_KEY);
    }

    public static final long MAX_UNLOCK_WAIT = 1000L;

    // https://stackoverflow.com/questions/19447444/fatal-errors-from-openjdk-when-running-fresh-jar-files
    public static void unlockFile(String jarFileName, long sz)
            throws IOException
    {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                long read = 0;
                try (FileInputStream fis = new FileInputStream(jarFileName)) {
                    byte[] buf = new byte[65536];
                    int anz;
                    while ((anz = fis.read(buf)) != -1) {
                        read += anz;
                    }
                }
                if (read != sz) {
                    throw new IllegalStateException();
                }
                return;
            }
            catch (Exception e) {
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed >= MAX_UNLOCK_WAIT) {
                    throw new RuntimeException("timeout while waiting for " + jarFileName);
                }

                try {
                    Thread.sleep(1);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted", ie);
                }
            }
        }
    }

    public static File tryGetCacheDep(String dep)
    {
        String s = System.getProperty(REPOSITORY_CACHE_PATH_PROPERTY_KEY);
        if (s == null || s.isEmpty()) {
            return null;
        }
        File cacheFile = new File(s, dep);
        if (!cacheFile.exists() || !cacheFile.isFile()) {
            return null;
        }
        return cacheFile;
    }

    public static void tryPutCacheDep(String dep, File depFile)
    {
        String s = System.getProperty(REPOSITORY_CACHE_PATH_PROPERTY_KEY);
        if (s == null || s.isEmpty()) {
            return;
        }
        File cacheFile = new File(s, dep);
        File parent = new File(cacheFile.getParent());
        parent.mkdirs();
        if (!(parent.exists() && parent.isDirectory())) {
            throw new IllegalStateException("Failed to make dir: " + parent.getAbsolutePath());
        }
    }

    public static List<URL> resolveUrlsForModule(ClassLoader sourceClassLoader, String moduleName)
            throws IOException
    {
        List<URL> urls = new ArrayList<>();
        File repositoryPath = getOrMakeRepositoryPath();
        Set<String> uncachedDeps = new HashSet<>();
        try (Scanner scanner = new Scanner(sourceClassLoader.getResourceAsStream("classpaths/.uncached"))) {
            while (scanner.hasNextLine()) {
                uncachedDeps.add(scanner.nextLine());
            }
        }
        String cachePath = System.getProperty(REPOSITORY_CACHE_PATH_PROPERTY_KEY);

        try (Scanner scanner = new Scanner(sourceClassLoader.getResourceAsStream("classpaths/" + moduleName))) {
            while (scanner.hasNextLine()) {
                String dep = scanner.nextLine();

                File depFile;
                if (cachePath != null && !cachePath.isEmpty() && !uncachedDeps.contains(dep)) {
                    depFile = new File(cachePath, dep);
                }
                else {
                    depFile = new File(repositoryPath, dep);
                }

                if (!depFile.exists()) {
                    File parent = depFile.getParentFile();
                    parent.mkdirs();
                    if (!(parent.exists() && parent.isDirectory())) {
                        throw new IOException("Failed to make dirs: " + parent.toString());
                    }
                    File tmp = new File(depFile.getAbsolutePath() + ".tmp");
                    long sz = 0;
                    try (InputStream bi = new BufferedInputStream(sourceClassLoader.getResourceAsStream("repository/" + dep));
                            OutputStream bo = new BufferedOutputStream(new FileOutputStream(tmp))) {
                        byte[] buf = new byte[65536];
                        int anz;
                        while ((anz = bi.read(buf)) != -1) {
                            bo.write(buf, 0, anz);
                            sz += anz;
                        }
                        bo.flush();
                    }
                    unlockFile(tmp.getAbsolutePath(), sz);
                    if (!tmp.renameTo(depFile)) {
                        throw new IOException(String.format("Rename failed: %s -> %s", tmp, depFile));
                    }
                }

                urls.add(depFile.toURL());
            }
        }

        return urls;
    }

    public static void setupClassLoaderForModule(ClassLoader sourceClassLoader, ClassLoader targetClassLoader, String moduleName)
            throws IOException
    {
        for (URL url : resolveUrlsForModule(sourceClassLoader, moduleName)) {
            addClasspathUrl(targetClassLoader, url);
        }
    }

    public static void setupClassLoaderForModule(ClassLoader classLoader, String moduleName)
            throws IOException
    {
        setupClassLoaderForModule(classLoader, classLoader, moduleName);
    }

    public static List<URL> resolveUrlsForModule(String moduleName)
            throws IOException
    {
        Thread[] threads = new Thread[1];
        Thread.enumerate(threads);
        ClassLoader classLoader = threads[0].getContextClassLoader(); // FIXME: lol.
        return resolveUrlsForModule(classLoader, moduleName);
    }

    public static void removeRecursive(Path path)
            throws IOException
    {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                try {
                    Files.delete(file);
                }
                catch (Exception e) {
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc)
                    throws IOException
            {
                // try to delete the file anyway, even if its attributes
                // could not be read, since delete-only access is
                // theoretically possible
                try {
                    Files.delete(file);
                }
                catch (Exception e) {
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException
            {
                if (exc == null) {
                    try {
                        Files.delete(dir);
                    }
                    catch (Exception e) {
                    }
                    return FileVisitResult.CONTINUE;
                }
                else {
                    // directory iteration failed; propagate exception
                    throw exc;
                }
            }
        });
    }
}
