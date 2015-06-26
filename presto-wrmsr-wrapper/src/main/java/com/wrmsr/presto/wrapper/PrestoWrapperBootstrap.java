package com.wrmsr.presto.wrapper;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.Scanner;

public class PrestoWrapperBootstrap
{
    /*
    public static class NestedClassloader extends ClassLoader
    {
        public NestedClassloader(ClassLoader parent)
        {
            super(parent);
        }

        public NestedClassloader()
        {
        }
    }
    */

    public static void main(String[] args) throws Throwable
    {
        ClassLoader classLoader = PrestoWrapperBootstrap.class.getClassLoader();
        try (Scanner scanner = new Scanner(classLoader.getResourceAsStream("classpaths/presto-wrmsr-wrapper"))) {
            while (scanner.hasNextLine()) {
                String dep = scanner.nextLine();
                System.out.println(dep);
                try (InputStream bi = new BufferedInputStream(classLoader.getResourceAsStream(dep))) {
                    byte[] buf = new byte[1024];
                    int anz;
                    while ((anz = bi.read(buf)) != -1) {
                        jo.write(buf, 0, anz);
                    }
                }
            }
        }

    }
}
