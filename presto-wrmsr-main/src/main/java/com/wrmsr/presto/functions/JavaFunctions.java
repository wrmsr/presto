package com.wrmsr.presto.functions;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;

public class JavaFunctions
{
    public static void main(String[] args)
            throws Throwable
    {
        // Prepare source somehow.
        String source = "package test; public class Test { static { System.out.println(\"hello\"); } public Test() { System.out.println(\"world\"); } }";

        // Save source in .java file.
        File root = Files.createTempDirectory(null).toFile();
        root.deleteOnExit(); // FIXME OSX EXECVE SEGFAULT
        File sourceFile = new File(root, "test/Test.java");
        sourceFile.getParentFile().mkdirs();
        Files.write(sourceFile.toPath(), source.getBytes());

        // Compile source file.
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        compiler.run(null, null, null, sourceFile.getPath());

        // Load and instantiate compiled class.
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] {root.toURI().toURL()});
        Class<?> cls = Class.forName("test.Test", true, classLoader); // Should print "hello".
        Object instance = cls.newInstance(); // Should print "world".
        System.out.println(instance); // Should print "test.Test@hashcode".
    }
}
