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
package com.wrmsr.presto.launcher.packaging.runtime;

import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestNestedJarFileSystem
{
    @Test
    public void testPathsGetAndDirectoryStream()
            throws IOException
    {
        Path root = Paths.get(URI.create("nestedjar:file://apache/kafka?revision=master!/"));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) {
            for (Path p : stream) {
                if (Files.isRegularFile(p)) {
                    System.out.println(p.toString());
                    Files.readAllBytes(p);
                }
                else {
                    System.out.println(p.toString() + "/");
                }
            }
        }
    }
}
