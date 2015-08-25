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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.launcher.util.ParentLastURLClassLoader;
import io.airlift.command.*;
import io.airlift.resolver.ArtifactResolver;
import jnr.posix.POSIX;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

/*
--heap
--debug-port
--jmx-port

--coordinator
--node-id
--random-node-id
--discovery-uri

com.facebook.presto.server.PluginManager=DEBUG
com.facebook.presto=INFO
com.ning.http.client=WARN
com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory=WARN
coordinator=false
coordinator=true
discovery-server.enabled=true
discovery.uri=http://example.net:8080
distributed-joins-enabled=true
exchange.http-client.connect-timeout=1m
exchange.http-client.max-connections-per-server=1000
exchange.http-client.max-connections=1000
exchange.http-client.read-timeout=1m
experimental-syntax-enabled=true
http-server.http.port=8080
node-scheduler.include-coordinator=false
node-scheduler.include-coordinator=true
node-scheduler.multiple-tasks-per-node-enabled=true
node.data-dir=/Users/wtimoney/presto/data/
node.environment=production
node.environment=test
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
plugin.dir=/dev/null
presto.version=0.105-SNAPSHOT
presto.version=testversion
query.client.timeout=5m
query.max-age=30m
scheduler.http-client.connect-timeout=1m
scheduler.http-client.max-connections-per-server=1000
scheduler.http-client.max-connections=1000
scheduler.http-client.read-timeout=1m
task.max-memory=1GB

s3
ec2
hdfs
*/

public class PrestoWrapperMain
{
    private PrestoWrapperMain()
    {
    }

    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Run.class,
                        Launch.class,
                        Start.class,
                        Stop.class,
                        Restart.class,
                        Status.class,
                        Kill.class,
                        CliCommand.class,
                        Hive.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    @Command(name = "launch", description = "Launches presto server (argless)")
    public static class Launch implements Runnable
    {
        private List<URL> classloaderUrls;

        public synchronized List<URL> getClassloaderUrls()
        {
            if (classloaderUrls == null) {
                classloaderUrls = ImmutableList.copyOf(resolveModuleClassloaderUrls("presto-main"));
            }
            return classloaderUrls;
        }
        @Override
        public void run()
        {
            runStaticMethod(getClassloaderUrls(), "com.facebook.presto.server.PrestoServer", "main", new Class<?>[]{String[].class}, new Object[]{new String[]{}});
        }
    }

    public static abstract class WrapperCommand implements Runnable
    {
        @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode")
        public boolean verbose;
        public static final String VERBOSE_PROPERTY_KEY = "wrmsr.launcher.verbose";

        @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
        public String configFile;
        public static final String CONFIG_FILE_PROPERTY_KEY = "wrmsr.launcher.config-file";

        public synchronized void deleteRepositoryOnExit()
        {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                File r = new File(Repositories.getRepositoryPath());
                checkState(r.exists() && r.isDirectory());
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run()
                    {
                        try {
                            Repositories.removeRecursive(r.toPath());
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
            }
        }
    }

    public static abstract class DaemonCommand extends WrapperCommand
    {
        @Option(name = {"-p", "--pid-file"}, description = "Specify pid file path")
        public String pidFile;
        public static final String PID_FILE_PROPERTY_KEY = "wrmsr.launcher.pid-file";

        private DaemonProcess daemonProcess;

        public synchronized boolean hasPidFile()
        {
            return !Strings.isNullOrEmpty(pidFile);
        }

        public synchronized DaemonProcess getDaemonProcess()
        {
            if (daemonProcess == null) {
                checkArgument(!Strings.isNullOrEmpty(pidFile), "must set pidfile");
                daemonProcess = new DaemonProcess(new File(pidFile));
            }
            return daemonProcess;
        }
    }

    public static abstract class ServerCommand extends DaemonCommand
    {
        @Option(name = {"-r", "--reexec"}, description = "Whether or not to reexec with appropriate JVM settings")
        public boolean reexec;

        @Option(name = {"-D"}, description = "Sets system property")
        public List<String> systemProperties = newArrayList();

        public String[] getExecArgs()
        {
            String java = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
            checkState(new File(java).exists());
            String jar;
            try {
                jar = new File(PrestoWrapperMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getAbsolutePath();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            checkState(new File(jar).exists());
            List<String> argv = newArrayList();
            // FIXME repo path + deleteOnExit
            argv.add(java);
            for (String s : systemProperties) {
                argv.add("-D" + s);
            }
            if (Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                argv.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
            }
            argv.add("-jar");
            argv.add(jar);
            argv.add("launch");
            return argv.toArray(new String[argv.size()]);
        }

        public void reexec()
        {
            POSIX posix = POSIXUtils.getPOSIX();
            String[] args = getExecArgs();
            posix.libc().execv(args[0], args);
        }

        public void launch()
        {
            Launch launch = reexec ? null : new Launch();
            if (Strings.isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
                System.setProperty("plugin.preloaded",  "|presto-wrmsr-main");
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.environment"))) {
                System.setProperty("node.environment",  "development");
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.id"))) {
                System.setProperty("node.id", UUID.randomUUID().toString());
            }
            if (Strings.isNullOrEmpty(System.getProperty("presto.version"))) {
                System.setProperty("presto.version", "0.113-SNAPSHOT");
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.coordinator"))) {
                System.setProperty("node.coordinator", "true");
            }
            if (Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                if (launch != null) {
                    launch.getClassloaderUrls();
                }
                String wd = new File(new File(System.getProperty("user.dir")), "presto-main").getAbsolutePath();
                File wdf = new File(wd);
                checkState(wdf.exists() && wdf.isDirectory());
                System.setProperty("user.dir", wd);
                POSIX posix = POSIXUtils.getPOSIX();
                checkState(posix.chdir(wd) == 0);
            }
            if (launch == null) {
                reexec();
            }
            else {
                for (String s : systemProperties) {
                    int i = s.indexOf('=');
                    System.setProperty(s.substring(0, i), s.substring(i + 1));
                }
                deleteRepositoryOnExit();
                launch.run();
            }
        }
    }

    @Command(name = "run", description = "Runs presto server")
    public static class Run extends ServerCommand
    {
        @Override
        public void run()
        {
            if (hasPidFile()) {
                getDaemonProcess().writePid();
            }
            launch();
        }
    }

    public abstract static class StartCommand extends ServerCommand
    {
        public void run(boolean restart)
        {
            if (hasPidFile()) {
                getDaemonProcess().writePid();
            }
            // launch(); dameonize
        }
    }

    @Command(name = "start", description = "Starts presto server")
    public static class Start extends StartCommand
    {
        @Override
        public void run()
        {
            run(false);
        }
    }

    @Command(name = "stop", description = "Stops presto server")
    public static class Stop extends DaemonCommand
    {
        @Override
        public void run()
        {
            getDaemonProcess().stop();
        }
    }

    @Command(name = "restart", description = "Restarts presto server")
    public static class Restart extends StartCommand
    {
        @Override
        public void run()
        {
            getDaemonProcess().stop();
            run(true);
        }
    }

    @Command(name = "status", description = "Gets status of presto server")
    public static class Status extends DaemonCommand
    {
        @Override
        public void run()
        {
            deleteRepositoryOnExit();
            if (!getDaemonProcess().alive()) {
                System.exit(DaemonProcess.LSB_NOT_RUNNING);
            }
            System.out.println(getDaemonProcess().readPid());
        }
    }

    @Command(name = "kill", description = "Kills presto server")
    public static class Kill extends DaemonCommand
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            deleteRepositoryOnExit();
            if (args.isEmpty()) {
                getDaemonProcess().kill();
            }
            else if (args.size() == 1) {
                int signal = Integer.valueOf(args.get(0));
                getDaemonProcess().kill(signal);
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public static List<URL> resolveModuleClassloaderUrls(String name)
    {
        try {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                return Repositories.resolveUrlsForModule(name);
            }
            else {
                ArtifactResolver resolver = new ArtifactResolver(
                        ArtifactResolver.USER_LOCAL_REPO,
                        ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));
                List<Artifact> artifacts = resolver.resolvePom(new File(name + "/pom.xml"));

                List<URL> urls = newArrayList();
                for (Artifact artifact : sortedArtifacts(artifacts)) {
                    if (artifact.getFile() == null) {
                        throw new RuntimeException("Could not resolve artifact: " + artifact);
                    }
                    File file = artifact.getFile().getCanonicalFile();
                    urls.add(file.toURI().toURL());
                }
                return urls;
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ClassLoader constructModuleClassloader(String name)
    {
        return new ParentLastURLClassLoader(resolveModuleClassloaderUrls(name));
    }

    public static void runStaticMethod(List<URL> urls, String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        Thread t = new Thread() {
            @Override
            public void run()
            {
                try {
                    ClassLoader cl = new URLClassLoader(urls.toArray(new URL[urls.size()]), getContextClassLoader().getParent());
                    Thread.currentThread().setContextClassLoader(cl);
                    Class cls = cl.loadClass(className);
                    Method main = cls.getMethod(methodName, parameterTypes);
                    main.invoke(null, args);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
        t.start();
        try {
            t.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    public static abstract class PassthroughCommand extends WrapperCommand
    {
        public abstract String getModuleName();

        public abstract String getClassName();

        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            deleteRepositoryOnExit();
            runStaticMethod(resolveModuleClassloaderUrls(getModuleName()), getClassName(), "main", new Class<?>[]{String[].class}, new Object[]{args.toArray(new String[args.size()])});
        }
    }

    @Command(name = "cli", description = "Starts presto cli")
    public static class CliCommand extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-cli";
        }

        @Override
        public String getClassName()
        {
            return "com.facebook.presto.cli.Presto";
        }
    }

    @Command(name = "hive", description = "Execute Hive command")
    public static class Hive extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-hadoop";
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.hadoop.HiveCli";
        }
    }

    @Command(name = "hdfs", description = "Executs HDFS command")
    public static class Hdfs extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-cli";
        }

        @Override
        public String getClassName()
        {
            return "com.facebook.presto.cli.Presto";
        }
    }

    /*
    @Command(name = "mesos", description = "Performs mesos operations")
    public static class Mesos extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }
    */

    /*
        @Arguments(description = "Patterns of files to be added")
        public List<String> patterns;

        @Option(name = "-i", description = "Add modified contents interactively.")
        public boolean interactive;

        @Option(name = "-n", description = "Do not query remote heads")
        public boolean noQuery;

        @Arguments(description = "Remote to show")
        public String remote;

        @Option(name = "-t", description = "Track only a specific branch")
        public String branch;

        @Arguments(description = "Remote repository to add")
        public List<String> remote;
    */
}
