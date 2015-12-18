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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.leacox.process.FinalizedProcessBuilder;
import com.sun.management.OperatingSystemMXBean;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.JvmConfig;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.config.LogConfig;
import com.wrmsr.presto.launcher.config.SystemConfig;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.log.Logger;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import io.airlift.units.DataSize;
import jnr.posix.POSIX;
import org.apache.commons.lang.math.IntRange;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Shell.shellEscape;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

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

public class LauncherMain
{
    private static final Logger log = Logger.get(LauncherMain.class);

    private LauncherMain()
    {
    }

    protected static String[] args; // meh.

    public static List<String> rewriteArgs(List<String> args)
    {
        List<String> ret = newArrayList();
        int i = 0;
        for (; i < args.size(); ++i ) {
            String s = args.get(i);
            if (!s.startsWith("-")) {
                break;
            }
            if (s.length() > 2 && Character.isUpperCase(s.charAt(1)) && s.contains("=")) {
                ret.add(s.substring(0, 2));
                ret.add(s.substring(2));
            }
            else {
                ret.add(s);
            }
        }
        for (; i < args.size(); ++i ) {
            ret.add(args.get(i));
        }
        return ret;
    }

    public static void main(String[] args)
            throws Throwable
    {
        // NameStore.getInstance().put("www.google.com", "www.microsoft.com");
        // InetAddress i = InetAddress.getAllByName("www.google.com")[0];
        // System.out.println(i);

        List<String> newArgs = rewriteArgs(Arrays.asList(args));
        args = newArgs.toArray(new String[newArgs.size()]);

        LauncherMain.args = args;

        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Run.class,
                        Daemon.class,
                        Launch.class,
                        Start.class,
                        Stop.class,
                        Restart.class,
                        Status.class,
                        Kill.class,
                        CliCommand.class,
                        H2.class,
                        Hive.class,
                        Hdfs.class,
                        Jython.class,
                        Nashorn.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    @Command(name = "launch", description = "Launches presto server (argless)")
    public static class Launch
            implements Runnable
    {
        private List<URL> classloaderUrls;

        public synchronized List<URL> getClassloaderUrls()
        {
            if (classloaderUrls == null) {
                classloaderUrls = ImmutableList.copyOf(resolveModuleClassloaderUrls("presto-main"));
            }
            return classloaderUrls;
        }

        private void autoConfigure()
        {
            if (Strings.isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
                System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
            }
        }

        @Override
        public void run()
        {
            autoConfigure();
            runStaticMethod(getClassloaderUrls(), "com.facebook.presto.server.PrestoServer", "main", new Class<?>[] {String[].class}, new Object[] {new String[] {}});
        }
    }

    private static String[] splitProperty(String prop)
    {
        int pos = prop.indexOf("=");
        checkArgument(pos > 0);
        return new String[] {prop.substring(0, pos), prop.substring(pos + 1)};
    }

    public static abstract class LauncherCommand
            implements Runnable
    {
        @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
        public List<String> configFiles = new ArrayList<>();

        @Option(type = OptionType.GLOBAL, name = {"-C"}, description = "Set config item")
        public List<String> configItems = new ArrayList<>();

        @Option(type = OptionType.GLOBAL, name = {"-D"}, description = "Sets system property")
        public List<String> systemProperties = newArrayList();

        private ConfigContainer config;

        public LauncherCommand()
        {
        }

        public synchronized ConfigContainer getConfig()
        {
            if (config == null) {
                for (String prop : configItems) {
                    String[] parts = splitProperty(prop);
                    PrestoConfigs.setConfigItem(parts[0], parts[1]);
                }
                config = PrestoConfigs.loadConfig(ConfigContainer.class, configFiles);
            }
            return config;
        }

        private void setArgSystemProperties()
        {
            for (String prop : systemProperties) {
                String[] parts = splitProperty(prop);
                System.setProperty(parts[0], parts[1]);
            }
        }

        private void setConfigSystemProperties()
        {
            for (Map.Entry<String, String> e : getConfig().getMergedNode(SystemConfig.class)) {
                System.setProperty(e.getKey(), e.getValue());
            }
        }

        private List<String> getJvmOptions()
        {
            return getJvmOptions(getConfig().getMergedNode(JvmConfig.class));
        }

        private List<String> getJvmOptions(JvmConfig jvmConfig)
        {
            if (jvmConfig.isAlreadyConfigured()) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<String> builder = ImmutableList.builder();

            if (jvmConfig.getDebugPort() != null) {
                builder.add(JvmConfiguration.DEBUG.valueOf().toString());
                builder.add(JvmConfiguration.REMOTE_DEBUG.valueOf(jvmConfig.getDebugPort(), jvmConfig.isDebugSuspend()).toString());
            }

            if (jvmConfig.getHeap() != null) {
                JvmConfig.HeapConfig heap = jvmConfig.getHeap();
                checkArgument(!Strings.isNullOrEmpty(heap.getSize()));
                OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                long base;
                if (heap.isFree()) {
                    base = os.getFreePhysicalMemorySize();
                }
                else {
                    base = os.getTotalPhysicalMemorySize();
                }

                long sz;
                if (heap.getSize().endsWith("%")) {
                    sz = ((base * 100) / Integer.parseInt(heap.getSize())) / 100;
                }
                else {
                    boolean negative = heap.getSize().startsWith("-");
                    sz = (long) DataSize.valueOf(heap.getSize().substring(negative ? 1 : 0)).getValue(DataSize.Unit.BYTE);
                    if (negative) {
                        sz = base - sz;
                    }
                }

                if (heap.getMax() != null) {
                    long max = (long) heap.getMax().getValue(DataSize.Unit.BYTE);
                    if (sz > max) {
                        sz = max;
                    }
                }
                if (heap.getMin() != null) {
                    long min = (long) heap.getMin().getValue(DataSize.Unit.BYTE);
                    if (sz < min) {
                        if (heap.isAttempt()) {
                            sz = min;
                        }
                        else {
                            throw new LauncherFailureException(String.format("Insufficient memory: got %d, need %d", sz, min));
                        }
                    }
                }
                checkArgument(sz > 0);
                builder.add(JvmConfiguration.MAX_HEAP_SIZE.valueOf(new DataSize(sz, DataSize.Unit.BYTE)).toString());
            }

            builder.addAll(jvmConfig.getOptions());

            return builder.build();
        }

        public static File getJarFile(Class cls)
        {
            File jar;
            try {
                jar = new File(cls.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
            }
            catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            checkState(jar.exists());
            return jar;
        }

        public static File getJvm()
        {
            File jvm = new File(System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : ""));
            checkState(jvm.exists() && jvm.isFile());
            return jvm;
        }

        private void maybeRexec()
        {
            List<String> jvmOptions = getJvmOptions();
            if (jvmOptions.isEmpty()) {
                return;
            }

            File jar = getJarFile(getClass());
            if (!jar.isFile()) {
                log.warn("Jvm options specified but not running with a jar file, ignoring");
                return;
            }

            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                    .addAll(jvmOptions)
                    .addAll(runtimeMxBean.getInputArguments())
                    .add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "jvm." + JvmConfig.ALREADY_CONFIGURED_KEY + "=true");
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
            }
            builder
                    .add("-jar")
                    .add(jar.getAbsolutePath())
                    .addAll(Arrays.asList(LauncherMain.args));

            List<String> newArgs = builder.build();
            POSIX posix = POSIXUtils.getPOSIX();
            File jvm = getJvm();
            posix.libc().execv(jvm.getAbsolutePath(), newArgs.toArray(new String[newArgs.size()]));
            throw new IllegalStateException("Unreachable");
        }

        private void configureLoggers()
        {
            for (Map.Entry<String, String> e : getConfig().getMergedNode(LogConfig.class).getEntries().entrySet()) {
                java.util.logging.Logger log = java.util.logging.Logger.getLogger(e.getKey());
                if (log != null) {
                    Level level = Level.parse(e.getValue().toUpperCase());
                    log.setLevel(level);
                }
            }
        }

        @Override
        public void run()
        {
            setArgSystemProperties();
            getConfig();
            configureLoggers();
            maybeRexec();
            setConfigSystemProperties();

            try {
                innerRun();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }

        public abstract void innerRun()
                throws Throwable;

        public synchronized void deleteRepositoryOnExit()
        {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                File r = new File(Repositories.getRepositoryPath());
                checkState(r.exists() && r.isDirectory());
                Runtime.getRuntime().addShutdownHook(new Thread()
                {
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

    public static abstract class DaemonCommand
            extends LauncherCommand
    {
        private DaemonProcess daemonProcess;

        public String pidFile()
        {
            return getConfig().getMergedNode(LauncherConfig.class).getPidFile();
        }

        public synchronized boolean hasPidFile()
        {
            return !Strings.isNullOrEmpty(pidFile());
        }

        public synchronized DaemonProcess getDaemonProcess()
        {
            if (daemonProcess == null) {
                checkArgument(!Strings.isNullOrEmpty(pidFile()), "must set pidfile");
                daemonProcess = new DaemonProcess(new File(pidFile()));
            }
            return daemonProcess;
        }
    }

    public static abstract class ServerCommand
            extends DaemonCommand
    {
        public void launch()
        {
            Launch launch = new Launch();
            if (Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                launch.getClassloaderUrls();
                String wd = new File(new File(System.getProperty("user.dir")), "presto-main").getAbsolutePath();
                File wdf = new File(wd);
                checkState(wdf.exists() && wdf.isDirectory());
                System.setProperty("user.dir", wd);
                POSIX posix = POSIXUtils.getPOSIX();
                checkState(posix.chdir(wd) == 0);
            }
            for (String s : systemProperties) {
                int i = s.indexOf('=');
                System.setProperty(s.substring(0, i), s.substring(i + 1));
            }
            deleteRepositoryOnExit();
            launch.run();
        }
    }

    @Command(name = "run", description = "Runs presto server")
    public static class Run
            extends ServerCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            launch();
        }
    }

    @Command(name = "daemon", description = "Runs presto server daemon")
    public static class Daemon
            extends ServerCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            getDaemonProcess().writePid();
            launch();
        }
    }

    public abstract static class StartCommand
            extends ServerCommand
    {
        public void run(boolean restart)
        {
            if (restart) {
                getDaemonProcess().stop();
            }
            List<String> args = ImmutableList.copyOf(LauncherMain.args);
            String lastArg = args.get(args.size() - 1);
            checkArgument(lastArg.equals("start") || lastArg.equals("restart"));

            File jvm = getJvm();
            ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                    .add(jvm.getAbsolutePath());
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            builder.addAll(runtimeMxBean.getInputArguments());
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
            }

            File jar = getJarFile(getClass());
            checkState(jar.isFile());

            builder
                    .add("-jar")
                    .add(jar.getAbsolutePath())
                    .addAll(IntStream.range(0, args.size() - 1).boxed().map(args::get).collect(toImmutableList()))
                    .add("daemon");

            ImmutableList.Builder<String> sh = ImmutableList.<String>builder()
                    .add("setsid")
                    .addAll(builder.build().stream().map(s -> shellEscape(s)).collect(toImmutableList()));
            sh.add("</dev/null");
            LauncherConfig config = getConfig().getMergedNode(LauncherConfig.class);
            for (String prefix : ImmutableList.of("", "2")) {
                if (!Strings.isNullOrEmpty(config.getLogFile())) {
                    sh.add(prefix + ">" + shellEscape(config.getLogFile()));
                }
                else {
                    sh.add(prefix + ">/dev/null");
                }
            }
            sh.add("&");

            String cmd = Joiner.on(" ").join(sh.build());

            List<String> newArgs = builder.build();
            POSIX posix = POSIXUtils.getPOSIX();
            posix.libc().execv(jvm.getAbsolutePath(), "sh", "-c", cmd);
            throw new IllegalStateException("Unreachable");
        }
    }

    @Command(name = "start", description = "Starts presto server")
    public static class Start
            extends StartCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            run(false);
        }
    }

    @Command(name = "stop", description = "Stops presto server")
    public static class Stop
            extends DaemonCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            getDaemonProcess().stop();
        }
    }

    @Command(name = "restart", description = "Restarts presto server")
    public static class Restart
            extends StartCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            getDaemonProcess().stop();
            run(true);
        }
    }

    @Command(name = "status", description = "Gets status of presto server")
    public static class Status
            extends DaemonCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            if (!getDaemonProcess().alive()) {
                System.exit(DaemonProcess.LSB_NOT_RUNNING);
            }
            System.out.println(getDaemonProcess().readPid());
        }
    }

    @Command(name = "kill", description = "Kills presto server")
    public static class Kill
            extends DaemonCommand
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void innerRun()
                throws Throwable
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
                artifacts = artifacts.stream().map(a -> {
                    for (String prefix : ImmutableList.of("", "../")) {
                        File f = new File(prefix + a.getArtifactId());
                        if (new File(f, "pom.xml").exists()) {
                            return new DefaultArtifact(
                                    a.getGroupId(),
                                    a.getArtifactId(),
                                    a.getClassifier(),
                                    a.getExtension(),
                                    a.getVersion(),
                                    a.getProperties(),
                                    new File(f, "target/classes"));
                        }
                    }
                    return a;
                }).collect(toImmutableList());

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

    public static void runStaticMethod(String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class cls = cl.loadClass(className);
            Method main = cls.getMethod(methodName, parameterTypes);
            main.invoke(null, args);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static void runStaticMethod(List<URL> urls, String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        Thread t = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    ClassLoader cl = new URLClassLoader(urls.toArray(new URL[urls.size()]), getContextClassLoader().getParent());
                    Thread.currentThread().setContextClassLoader(cl);
                    runStaticMethod(className, methodName, parameterTypes, args);
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
        }
    }

    public static abstract class PassthroughCommand
            extends LauncherCommand
    {
        public abstract String getModuleName();

        public abstract String getClassName();

        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void innerRun()
                throws Throwable
        {
            deleteRepositoryOnExit();
            String moduleName = getModuleName();
            Class<?>[] parameterTypes = new Class<?>[] {String[].class};
            Object[] args = new Object[] {this.args.toArray(new String[this.args.size()])};
            if (moduleName == null) {
                runStaticMethod(getClassName(), "main", parameterTypes, args);
            }
            else {
                runStaticMethod(resolveModuleClassloaderUrls(moduleName), getClassName(), "main", parameterTypes, args);
            }
        }
    }

    @Command(name = "cli", description = "Starts presto cli")
    public static class CliCommand
            extends PassthroughCommand
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

    @Command(name = "hive", description = "Executes Hive command")
    public static class Hive
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-hadoop";
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.hadoop.hive.HiveMain";
        }
    }

    @Command(name = "hdfs", description = "Executes HDFS command")
    public static class Hdfs
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-hadoop";
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.hadoop.hdfs.HdfsMain";
        }
    }

    @Command(name = "h2", description = "Execute H2 command")
    public static class H2
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return null;
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.launcher.H2Main";
        }
    }

    @Command(name = "jython", description = "Starts Jython shell")
    public static class Jython
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-jython";
        }

        @Override
        public String getClassName()
        {
            return "org.python.util.jython";
        }
    }

    @Command(name = "nashorn", description = "Starts Nashorn shell")
    public static class Nashorn
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return null;
        }

        @Override
        public String getClassName()
        {
            return "jdk.nashorn.tools.Shell";
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
