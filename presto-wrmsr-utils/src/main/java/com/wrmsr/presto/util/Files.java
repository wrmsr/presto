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
package com.wrmsr.presto.util;

import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import static java.nio.file.Files.readAllBytes;

public class Files
{
    private Files()
    {
    }

    public static void writeFileBytes(String path, byte[] content)
            throws IOException
    {
        java.nio.file.Files.write(FileSystems.getDefault().getPath(path), content);
    }

    public static byte[] readFileBytes(String path)
            throws IOException
    {
        return readAllBytes(FileSystems.getDefault().getPath(path));
    }

    public static void writeFile(String path, String content)
            throws IOException
    {
        try (BufferedWriter bw = java.nio.file.Files.newBufferedWriter(
                FileSystems.getDefault().getPath(path),
                StandardCharsets.UTF_8)) {
            bw.write(content);
        }
    }

    public static String readFile(String path)
            throws IOException
    {
        try (BufferedReader br = java.nio.file.Files.newBufferedReader(
                FileSystems.getDefault().getPath(path),
                StandardCharsets.UTF_8)) {
            return CharStreams.toString(br);
        }
    }

    public static void downloadFile(String url, File path)
            throws IOException
    {
        try (ReadableByteChannel rbc = Channels.newChannel(new URL(url).openStream());
                FileOutputStream fos = new FileOutputStream(path)) {
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        }
    }

    public static void makeDirsAndCheck(File f)
    {
        f.mkdirs();
        if (!(f.exists() && f.isDirectory())) {
            throw new IllegalStateException("Failed to make dir: " + f.getAbsolutePath());
        }
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param filename The filename to convert
     * @return The unix pathname
     * @throws IOException on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(String filename)
            throws IOException
    {
        return filename;
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param file The filename to convert
     * @return The unix pathname
     * @throws IOException on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(File file)
            throws IOException
    {
        return makeShellPath(file, false);
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param file The filename to convert
     * @param makeCanonicalPath Whether to make canonical path for the file passed
     * @return The unix pathname
     * @throws IOException on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(File file, boolean makeCanonicalPath)
            throws IOException
    {
        if (makeCanonicalPath) {
            return makeShellPath(file.getCanonicalPath());
        }
        else {
            return makeShellPath(file.toString());
        }
    }

    public static byte[] readAllBytesNoThrow(Path path)
    {
        try {
            return readAllBytes(path);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

//    /**
//     * Takes an input dir and returns the du on that local directory. Very basic
//     * implementation.
//     *
//     * @param dir
//     *          The input dir to get the disk space of this local dir
//     * @return The total disk space of the input local directory
//     */
//    public static long getDU(File dir)
//    {
//        long size = 0;
//        if (!dir.exists()) {
//            return 0;
//        }
//        if (!dir.isDirectory()) {
//            return dir.length();
//        }
//        else {
//            File[] allFiles = dir.listFiles();
//            if (allFiles != null) {
//                for (int i = 0; i < allFiles.length; i++) {
//                    boolean isSymLink;
//                    try {
//                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
//                    }
//                    catch (IOException ioe) {
//                        isSymLink = true;
//                    }
//                    if (!isSymLink) {
//                        size += getDU(allFiles[i]);
//                    }
//                }
//            }
//            return size;
//        }
//    }
}
