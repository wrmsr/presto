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
package com.wrmsr.presto.launcher;

import com.google.common.base.Throwables;
import com.wrmsr.presto.launcher.util.JarSync;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;

import java.io.File;
import java.nio.file.Files;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Files.makeDirsAndCheck;
import static com.wrmsr.presto.util.Jvm.getThisJarFile;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class JarSyncMain
{
    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Send.class,
                        Receive.class
                );

        Cli<Runnable> cliParser = builder.build();
        cliParser.parse(args).run();
    }

    @Command(name = "send", description = "Sends jarsync via stdio")
    public static class Send
            implements Runnable
    {
        @Option(name = "--src")
        public String src;

        @Override
        public void run()
        {
            File jar = getThisJarFile(getClass());

            File src = isNullOrEmpty(this.src) ? jar : new File(this.src);
            checkState(src.isFile());

            try {
                JarSync.Driver driver = new JarSync.SourceDriver(src);
                driver.run(OBJECT_MAPPER.get(), System.in, System.out);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Command(name = "receive", description = "Receives jarsync via stdio")
    public static class Receive
            implements Runnable
    {
        @Option(name = "--src")
        public String src;

        @Option(name = "--dst")
        public String dst;

        @Override
        public void run()
        {
            File jar = getThisJarFile(getClass());

            File src = isNullOrEmpty(this.src) ? jar : new File(this.src);
            checkState(src.isFile());

            File dst = isNullOrEmpty(this.dst) ? jar : new File(this.dst);
            if (dst.getParentFile() != null) {
                makeDirsAndCheck(dst.getParentFile());
            }

            try {
                File tmpDir = Files.createTempDirectory(null).toFile();
                tmpDir.deleteOnExit();
                File tmp = new File(tmpDir, dst.getName());

                JarSync.Driver driver = new JarSync.SinkDriver(src, tmp);
                driver.run(OBJECT_MAPPER.get(), System.in, System.out);

                if (tmp.renameTo(dst)) {
                    throw new RuntimeException(String.format("Rename failed: %s -> %s", tmp, dst));
                }
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
