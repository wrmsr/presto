package com.wrmsr.presto.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Repositories
{
    private Repositories()
    {
    }

    public static final String REPOSITORY_PATH_PROPERTY_KEY = "wrmsr.repository.path";
    public static final String SHOULD_DELETE_REPOSITORY_PROPERTY_KEY = "wrmsr.repository.should-delete";

    public static void addClasspathUrl(URLClassLoader classLoader, URL url) throws IOException
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

    public static void addClasspathUrl(ClassLoader classLoader, URL url) throws IOException
    {
        addClasspathUrl((URLClassLoader) classLoader, url);
    }

    public static File getOrMakePropertyPath(String key) throws IOException
    {
        String repositoryPathString = System.getProperty(key);
        File repositoryPath;
        if (repositoryPathString == null || repositoryPathString.isEmpty()) {
            repositoryPath = Files.createTempDirectory(null).toFile();
            repositoryPath.deleteOnExit(); // FIXME OSX EXECVE SEGFAULT
            System.setProperty(key, repositoryPath.getAbsolutePath());
        }
        else {
            repositoryPath = new File(repositoryPathString);
            if (!repositoryPath.exists()) {
                throw new IOException("Repository path does not exist: " + repositoryPath);
            }
            if (!repositoryPath.isDirectory()) {
                throw new IOException("Repository path is not a directory: " + repositoryPath);
            }
            if (Boolean.valueOf(System.getProperty(SHOULD_DELETE_REPOSITORY_PROPERTY_KEY))) {
                repositoryPath.deleteOnExit();
            }
        }
        return repositoryPath;
    }

    public static File getRepositoryPath() throws IOException
    {
        return getOrMakePropertyPath(REPOSITORY_PATH_PROPERTY_KEY);
    }

    public static List<URL> resolveUrlsForModule(ClassLoader sourceClassLoader, String moduleName) throws IOException
    {
        List<URL> urls = new ArrayList<>();
        File repositoryPath = getRepositoryPath();

        try (Scanner scanner = new Scanner(sourceClassLoader.getResourceAsStream("classpaths/" + moduleName))) {
            while (scanner.hasNextLine()) {
                String dep = scanner.nextLine();
                File depFile = new File(repositoryPath, dep);
                depFile.getParentFile().mkdirs();
                try (InputStream bi = new BufferedInputStream(sourceClassLoader.getResourceAsStream(dep));
                     OutputStream bo = new BufferedOutputStream(new FileOutputStream(depFile))) {
                    byte[] buf = new byte[65536];
                    int anz;
                    while ((anz = bi.read(buf)) != -1) {
                        bo.write(buf, 0, anz);
                    }
                }
                urls.add(depFile.toURL());
            }
        }

        return urls;
    }

    public static void setupClassLoaderForModule(ClassLoader sourceClassLoader,ClassLoader targetClassLoader, String moduleName) throws IOException
    {
        for (URL url : resolveUrlsForModule(sourceClassLoader, moduleName)) {
            addClasspathUrl(targetClassLoader, url);
        }
    }

    public static void setupClassLoaderForModule(ClassLoader classLoader, String moduleName) throws IOException
    {
        setupClassLoaderForModule(classLoader, classLoader, moduleName);
    }

    public static List<URL> resolveUrlsForModule(String moduleName) throws IOException
    {
        Thread[] threads = new Thread[1];
        Thread.enumerate(threads);
        ClassLoader classLoader = threads[0].getContextClassLoader();
        return resolveUrlsForModule(classLoader, moduleName);
    }
}
