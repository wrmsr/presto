package com.wrmsr.presto.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class Jvm
{
    private Jvm()
    {
    }

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
}
