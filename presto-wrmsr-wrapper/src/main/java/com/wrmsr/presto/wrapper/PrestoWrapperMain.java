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
package com.wrmsr.presto.wrapper;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.wrapper.util.POSIXUtils;
import com.wrmsr.presto.wrapper.util.ParentLastURLClassLoader;
import io.airlift.command.*;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.log.Logger;
import jnr.posix.POSIX;
import jnr.posix.util.Platform;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

// mesos yarn cli jarsync

public class PrestoWrapperMain
{
    public static class Process
    {
        private final File path;
        private final int pidFile;
        private final POSIX posix;

        private boolean locked;

        public Process(File path)
        {
            final int openFlags;
            if (Platform.IS_MAC) {
                openFlags =
                        jnr.constants.platform.darwin.OpenFlags.O_RDWR.intValue() |
                        jnr.constants.platform.darwin.OpenFlags.O_CREAT.intValue();
            }
            else if (Platform.IS_LINUX) {
                openFlags =
                        jnr.constants.platform.linux.OpenFlags.O_RDWR.intValue() |
                        jnr.constants.platform.linux.OpenFlags.O_CREAT.intValue();
            }
            else {
                throw new IllegalStateException("Unsupported platform");
            }
            posix = POSIXUtils.getPOSIX();
            this.path = path;
            path.getParentFile().mkdirs();
            pidFile = posix.open(path.getAbsolutePath(), openFlags, 0600);
            refresh();
        }

        private void refresh()
        {
            try {
                posix.flock(pidFile, 2 | 4); // LOCK_EX | LOCK_NB
                locked = true;
            }
            catch (Exception e) {
                locked = false;
            }
        }

        private void clearPid()
        {
            checkState(locked, "pid file not locked by us");
            posix.lseek(pidFile, 0, 0);
            posix.ftruncate(pidFile, 0);
        }

        private void writePid(int pid)
        {
            clearPid();
            byte[] bytes = String.format("%d\n", pid).getBytes();
            posix.write(pidFile, bytes, bytes.length);
            posix.fsync(pidFile);
        }

        private boolean alive()
        {
            refresh();
            if (locked) {
                return false;
            }
            int pid = readPid();
            try {
                posix.kill(pid, 0);
                return true;
            }
            catch (Exception e) {
                throw new IllegalStateException(String.format("Signaling pid %s failed: %s", pid, e));
            }
        }

        private int readPid()
        {
            checkState(!locked, "pid file is locked by us");
            posix.lseek(pidFile, 0, 0);
            byte[] buf = new byte[1024];
            int len = posix.read(pidFile, buf, buf.length);
            checkState(len > 0 && len < buf.length);
            int pid = Integer.valueOf(new String(Arrays.copyOf(buf, len)));
            checkState(pid > 0);
            return pid;
        }
    }

    public static void main2(String[] args)
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, Run.class, Start.class, Stop.class, Restart.class, Status.class, Kill.class, CliCommand.class);

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class WrapperCommand implements Runnable
    {
        @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode")
        public boolean verbose;

        @Option(name = {"-c", "--config"}, description = "Specify config file path")
        public String configFile;
    }

    public static abstract class ServerCommand extends WrapperCommand
    {
        @Option(name = {"-p", "--pidfile"}, description = "Specify pidfile path")
        public String pidFile;
    }

    @Command(name = "run", description = "Runs presto server")
    public static class Run extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "start", description = "Starts presto server")
    public static class Start extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "stop", description = "Stops presto server")
    public static class Stop extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "restart", description = "Restarts presto server")
    public static class Restart extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "status", description = "Gets status of presto server")
    public static class Status extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "kill", description = "Kills presto server")
    public static class Kill extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "cli", description = "Starts presto cli")
    public static class CliCommand extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }

    /*
    @Command(name = "mesos", description = "Performs mesos operations")
    public static class Mesos extends ServerCommand
    {
        @Override
        public void run()
        {

        }
    }
    */

    /*
    @Command(name = "add", description = "Add file contents to the index")
    public static class Add extends WrapperCommand
    {
        @Arguments(description = "Patterns of files to be added")
        public List<String> patterns;

        @Option(name = "-i", description = "Add modified contents interactively.")
        public boolean interactive;
    }

    @Command(name = "show", description = "Gives some information about the remote <name>")
    public static class RemoteShow extends WrapperCommand
    {
        @Option(name = "-n", description = "Do not query remote heads")
        public boolean noQuery;

        @Arguments(description = "Remote to show")
        public String remote;
    }

    @Command(name = "add", description = "Adds a remote")
    public static class RemoteAdd extends WrapperCommand
    {
        @Option(name = "-t", description = "Track only a specific branch")
        public String branch;

        @Arguments(description = "Remote repository to add")
        public List<String> remote;
    }
    */

    private PrestoWrapperMain()
    {
    }

    public static void main(String[] args)
            throws Throwable
    {
        //new PrestoWrapperMain().run(args);
        main2(args);
    }

    public void run(String[] args)
            throws Throwable
    {
        /*
        Logger log = Logger.get(PrestoWrapperMain.class);

        ImmutableMap<String, String> props = ImmutableMap.<String, String>builder()
                .put("node.environment", "production")
                .put("node.id", "ffffffff-ffff-ffff-ffff-ffffffffffff")
                .put("node.data-dir", "/Users/wtimoney/presto/data/")
                .put("presto.version", "0.105-SNAPSHOT")
                .put("coordinator", "true")
                .put("node-scheduler.include-coordinator", "true")
                .put("http-server.http.port", "8080")
                .put("task.max-memory", "1GB")
                .put("discovery-server.enabled", "true")
                .put("discovery.uri", "http://localhost:8080")
                .build();
        for (Map.Entry<String, String> e : props.entrySet()) {
            System.setProperty(e.getKey(), e.getValue());
        }

        File cwd = new File(System.getProperty("user.dir"));

        List<URL> urls = Lists.newArrayList();
        boolean isShaded = false;

        if (isShaded) {
            File shaded = new File(cwd, "presto-main/target/presto-main.jar");
            if (!shaded.exists()) {
                throw new IllegalStateException(shaded.getAbsolutePath());
            }
            URL url = new URL("file:" + shaded.getAbsolutePath());
            urls.add(url);

        } else {
            ArtifactResolver resolver = new ArtifactResolver(
                    ArtifactResolver.USER_LOCAL_REPO,
                    ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

            List<Artifact> artifacts = resolver.resolvePom(new File(cwd, "presto-main/pom.xml"));

            List<File> files = Lists.newArrayList(
                    new File(cwd, "presto-main/target/presto-main.jar"),
                    new File(cwd, "presto-main/target/classes/")
            );
            for (Artifact a : artifacts) {
                files.add(a.getFile());
            }

            for (File file : files) {
                if (!file.exists()) {
                    // throw new IllegalStateException(file.getAbsolutePath());
                }
                URL url = new URL("file:" + file.getAbsolutePath() + (file.isDirectory() ? "/" : ""));
                System.out.println(url);
                urls.add(url);
            }
        }

        ClassLoader cl = new ParentLastURLClassLoader(urls);

        Class prestoServerClass = cl.loadClass("com.facebook.presto.server.PrestoServer");
        Method prestoServerMain = prestoServerClass.getMethod("main", String[].class);
        prestoServerMain.invoke(null, new Object[]{ args });
        */
        Thread thread = new Thread() {
            @Override
            public void run()
            {
                try {
                    ClassLoader originalCl = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[]{}, originalCl.getParent()));
                    ParentLastURLClassLoader cl = new ParentLastURLClassLoader(ImmutableList.of());
                    Repositories.setupClassLoaderForModule(originalCl, cl.getChildClassLoader(), "presto-main");
                    Class prestoServerClass = cl.loadClass("com.facebook.presto.server.PrestoServer");
                    Method prestoServerMain = prestoServerClass.getMethod("main", String[].class);
                    prestoServerMain.invoke(null, new Object[]{args});
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
        thread.start();
        thread.join();
    }
}
