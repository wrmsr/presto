package com.wrmsr.presto.util;

import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;

public class Files
{
    private Files()
    {
    }

    public static String readFile(String path) throws IOException
    {
        try (BufferedReader br = java.nio.file.Files.newBufferedReader(
                FileSystems.getDefault().getPath(path),
                StandardCharsets.UTF_8)) {
            return CharStreams.toString(br);
        }
    }
}
