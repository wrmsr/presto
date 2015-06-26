package com.wrmsr.presto.wrapper;

import java.util.Scanner;

public class PrestoWrapperBootstrap
{
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

    public static void main(String[] args) throws Throwable
    {
        ClassLoader classLoader = PrestoWrapperBootstrap.class.getClassLoader();
        try (Scanner scanner = new Scanner(classLoader.getResourceAsStream("classpaths/presto-wrmsr-wrapper"))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
            }
        }
    }
}
