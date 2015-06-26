package com.wrmsr.presto.wrapper;

import java.io.File;
import java.util.Scanner;

public class PrestoWrapperBootstrap
{
    public static void main(String[] args) throws Throwable
    {
        ClassLoader classLoader = PrestoWrapperBootstrap.class.getClassLoader();
        File file = new File(classLoader.getResource("classpaths/presto-wrmsr-wrapper").getFile());
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                System.out.println(line);
            }
        }
    }
}
