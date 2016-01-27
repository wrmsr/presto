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
package com.wrmsr.presto.launcher.commands;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.management.OperatingSystemMXBean;
import com.wrmsr.presto.launcher.LauncherFailureException;
import com.wrmsr.presto.launcher.LauncherUtils;
import com.wrmsr.presto.launcher.cluster.ClusterConfig;
import com.wrmsr.presto.launcher.cluster.ClusterUtils;
import com.wrmsr.presto.launcher.cluster.ClustersConfig;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.EnvConfig;
import com.wrmsr.presto.launcher.config.JvmConfig;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.config.SystemConfig;
import com.wrmsr.presto.launcher.logging.LoggingConfig;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Artifacts;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.log.SubprocessHandler;
import io.airlift.units.DataSize;
import jnr.posix.POSIX;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Files.makeDirsAndCheck;
import static com.wrmsr.presto.util.Jvm.getJarFile;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.ShellUtils.shellEscape;
import static com.wrmsr.presto.util.Strings.replaceStringVars;
import static com.wrmsr.presto.util.Strings.splitProperty;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public abstract class AbstractLauncherCommand
        implements LauncherCommand
{
    private static final Logger log = Logger.get(AbstractLauncherCommand.class);

    // TODO bootstrap in run, member inject
    public static final ThreadLocal<String[]> ORIGINAL_ARGS = new ThreadLocal<>();

    @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
    public List<String> configFiles = new ArrayList<>();

    @Option(type = OptionType.GLOBAL, name = {"-C"}, description = "Set config item")
    public List<String> configItems = new ArrayList<>();

    @Option(type = OptionType.GLOBAL, name = {"-D"}, description = "Sets system property")
    public List<String> systemProperties = newArrayList();

    private ConfigContainer config;

    private volatile POSIX posix;

    public synchronized POSIX getPosix()
    {
        if (posix == null) {
            posix = POSIXUtils.getPOSIX();
        }
        return posix;
    }

    public AbstractLauncherCommand()
    {
    }

    @Override
    public synchronized ConfigContainer getConfig()
    {
        if (this.config == null) {
            for (String prop : configItems) {
                String[] parts = splitProperty(prop);
                PrestoConfigs.setConfigItem(parts[0], parts[1]);
            }
            ConfigContainer config = PrestoConfigs.loadConfig(getClass(), ConfigContainer.class, configFiles);
            setConfigEnv(config);
            setConfigSystemProperties(config);

            LauncherConfig launcherConfig = config.getMergedNode(LauncherConfig.class);
            ClustersConfig clustersConfig = config.getMergedNode(ClustersConfig.class);
            if (!isNullOrEmpty(launcherConfig.getClusterNameFile())) {
                File clusterNameFile = new File(replaceVars(launcherConfig.getClusterNameFile()));
                if (clusterNameFile.exists()) {
                    String clusterName;
                    try {
                        clusterName = new String(Files.readAllBytes(clusterNameFile.toPath())).trim();
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                    ClusterConfig clusterConfig = clustersConfig.getEntries().get(clusterName);

                    if (clusterConfig.getConfig() != null) {
                        ConfigContainer updates = Serialization.roundTrip(OBJECT_MAPPER.get(), clusterConfig.getConfig(), ConfigContainer.class);
                        config = (ConfigContainer) config.merge(updates);
                        config = (ConfigContainer) config.merge(
                                Serialization.roundTrip(OBJECT_MAPPER.get(), ImmutableList.of(ImmutableMap.of("launcher", ImmutableMap.of("cluster-name", clusterName))), ConfigContainer.class));
                        setConfigEnv(config);
                        setConfigSystemProperties(config);
                    }

                    if (!isNullOrEmpty(launcherConfig.getClusterNodeNameFile())) {
                        File clusterNodeNameFile = new File(replaceVars(launcherConfig.getClusterNodeNameFile()));
                        if (clusterNodeNameFile.exists()) {
                            String clusterNodeName;
                            try {
                                clusterNodeName = new String(Files.readAllBytes(clusterNodeNameFile.toPath())).trim();
                            }
                            catch (IOException e) {
                                throw Throwables.propagate(e);
                            }

                            List clusterNodeConfig = ClusterUtils.getNodeConfig(clusterConfig, clusterNodeName);
                            if (clusterNodeConfig != null) {
                                ConfigContainer updates = Serialization.roundTrip(OBJECT_MAPPER.get(), clusterNodeConfig, ConfigContainer.class);
                                config = (ConfigContainer) config.merge(updates);
                                config = (ConfigContainer) config.merge(
                                        Serialization.roundTrip(OBJECT_MAPPER.get(), ImmutableList.of(ImmutableMap.of("launcher", ImmutableMap.of("cluster-node-name", clusterNodeName))), ConfigContainer.class));
                                setConfigEnv(config);
                                setConfigSystemProperties(config);
                            }
                        }
                    }
                }
            }

            PrestoConfigs.writeConfigProperties(config, this::replaceVars);
            this.config = config;
        }
        return this.config;
    }

    private void setArgSystemProperties()
    {
        for (String prop : systemProperties) {
            String[] parts = splitProperty(prop);
            System.setProperty(parts[0], parts[1]);
        }
    }

    private void setConfigSystemProperties(ConfigContainer config)
    {
        for (SystemConfig c : config.getNodes(SystemConfig.class)) {
            for (Map.Entry<String, String> e : c) {
                System.setProperty(e.getKey(), replaceVars(e.getValue()));
            }
        }
    }

    private String getEnvOrSystemProperty(String key)
    {
        String value = getPosix().getenv(key.toUpperCase());
        if (value != null) {
            return value;
        }
        return System.getProperty(key);
    }

    @Override
    public String replaceVars(String text)
    {
        return replaceStringVars(text, this::getEnvOrSystemProperty);
    }

    private void setConfigEnv(ConfigContainer config)
    {
        for (EnvConfig c : config.getNodes(EnvConfig.class)) {
            for (Map.Entry<String, String> e : c) {
                getPosix().libc().setenv(e.getKey(), replaceVars(e.getValue()), 1);
            }
        }
    }

    private void ensureConfigDirs()
    {
        for (String dir : firstNonNull(config.getMergedNode(LauncherConfig.class).getEnsureDirs(), ImmutableList.<String>of())) {
            makeDirsAndCheck(new File(replaceVars(dir)));
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

        JvmConfig.DebugConfig debug = jvmConfig.getDebug();
        if (debug != null && debug.getPort() != null) {
            builder.add(JvmConfiguration.DEBUG.valueOf().toString());
            builder.add(JvmConfiguration.REMOTE_DEBUG.valueOf(debug.getPort(), debug.isSuspend()).toString());
        }

        if (jvmConfig.getHeap() != null) {
            JvmConfig.HeapConfig heap = jvmConfig.getHeap();
            checkArgument(!isNullOrEmpty(heap.getSize()));
            OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
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

        if (jvmConfig.getGc() != null) {
            JvmConfig.GcConfig gc = jvmConfig.getGc();
            if (gc instanceof JvmConfig.G1GcConfig) {
                builder.add("-XX:+UseG1GC");
            }
            else if (gc instanceof JvmConfig.CmsGcConfig) {
                builder.add("-XX:+UseConcMarkSweepGC");
            }
            else {
                throw new IllegalArgumentException(gc.toString());
            }
        }

        builder.addAll(jvmConfig.getOptions());

        return builder.build();
    }

    public static File getJvm()
    {
        File jvm = new File(System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : ""));
        checkState(jvm.exists() && jvm.isFile());
        return jvm;
    }

    protected void maybeRexec()
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

        File jvm = getJvm();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                .add(jvm.getAbsolutePath())
                .addAll(jvmOptions)
                .addAll(runtimeMxBean.getInputArguments())
                .add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "jvm." + JvmConfig.ALREADY_CONFIGURED_KEY + "=true");
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
        }
        builder
                .add("-jar")
                .add(jar.getAbsolutePath())
                .addAll(Arrays.asList(ORIGINAL_ARGS.get()));

        List<String> newArgs = builder.build();
        getPosix().libc().execv(jvm.getAbsolutePath(), newArgs.toArray(new String[newArgs.size()]));
        throw new IllegalStateException("Unreachable");
    }

    public void configureLoggers()
    {
        Logging logging = Logging.initialize();
        try {
            logging.configure(new LoggingConfiguration());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        System.setProperty("presto.do-not-initialize-logging", "true");

        LoggingConfig lc = getConfig().getMergedNode(LoggingConfig.class);

        for (Map.Entry<String, String> e : lc.getLevels().entrySet()) {
            java.util.logging.Logger log = java.util.logging.Logger.getLogger(e.getKey());
            if (log != null) {
                Level level = Level.parse(e.getValue().toUpperCase());
                log.setLevel(level);
            }
        }

        for (LoggingConfig.AppenderConfig ac : lc.getAppenders()) {
            if (ac instanceof LoggingConfig.SubprocessAppenderConfig) {
                LoggingConfig.SubprocessAppenderConfig sac = (LoggingConfig.SubprocessAppenderConfig) ac;
                java.util.logging.Logger.getLogger("").addHandler(new SubprocessHandler(sac.getArgs()));

            }
            else {
                throw new IllegalStateException();
            }
        }
    }

    public boolean shouldDeleteRepository()
    {
        return true;
    }

    private void autoConfigure()
    {
        LauncherConfig lc = getConfig().getMergedNode(LauncherConfig.class);
        if (isNullOrEmpty(System.getProperty("http-server.log.path"))) {
            if (!isNullOrEmpty(lc.getHttpLogFile())) {
                System.setProperty("http-server.log.path", replaceVars(lc.getHttpLogFile()));
            }
        }
    }

    @Override
    public void run()
    {
        setArgSystemProperties();
        getConfig();

        ensureConfigDirs();
        if (shouldDeleteRepository()) {
            deleteRepositoryOnExit();
        }
        autoConfigure();

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
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
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

    private DaemonProcess daemonProcess;

    private String pidFile()
    {
        return getConfig().getMergedNode(LauncherConfig.class).getPidFile();
    }

    public synchronized DaemonProcess getDaemonProcess()
    {
        if (daemonProcess == null) {
            checkArgument(!isNullOrEmpty(pidFile()), "must set pidfile");
            daemonProcess = new DaemonProcess(new File(replaceVars(pidFile())), getConfig().getMergedNode(LauncherConfig.class).getPidFileFd());
        }
        return daemonProcess;
    }

    public void launch()
    {
        configureLoggers();
        if (isNullOrEmpty(Repositories.getRepositoryPath())) {
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
        long delay = getConfig().getMergedNode(LauncherConfig.class).getDelay();
        if (delay > 0) {
            log.info(String.format("Delaying launch for %d ms", delay));
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        launchLocal();
    }

    public void launchLocal()
    {
        if (isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
            System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
        }

        List<URL> classloaderUrls = ImmutableList.copyOf(Artifacts.resolveModuleClassloaderUrls("presto-main"));

        try {
            LauncherUtils.runStaticMethod(classloaderUrls, "com.facebook.presto.server.PrestoServer", "main", new Class<?>[] {String[].class}, new Object[] {new String[] {}});
        }
        catch (Throwable e) {
            try {
                log.error(e);
            }
            catch (Throwable e2) {
            }
            System.exit(1);
        }
    }

    public void runServer(boolean restart)
    {
        if (getDaemonProcess().alive()) {
            if (restart) {
                getDaemonProcess().stop();
            }
            else {
                return;
            }
        }

        List<String> args = ImmutableList.copyOf(ORIGINAL_ARGS.get());
        String lastArg = args.get(args.size() - 1);
        checkArgument(lastArg.equals("start") || lastArg.equals("restart"));

        File jvm = getJvm();
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                .add(jvm.getAbsolutePath());
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        // builder.addAll(runtimeMxBean.getInputArguments());
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
        }

        builder.add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "launcher." + LauncherConfig.PID_FILE_FD_KEY + "=" + getDaemonProcess().pidFile);

        LauncherConfig config = getConfig().getMergedNode(LauncherConfig.class);

        if (!isNullOrEmpty(config.getLogFile())) {
            builder.add("-Dlog.output-file=" + replaceVars(config.getLogFile()));
            builder.add("-Dlog.enable-console=false");
        }

        File jar = getJarFile(getClass());
        checkState(jar.isFile());

        builder
                .add("-jar")
                .add(jar.getAbsolutePath())
                .addAll(IntStream.range(0, args.size() - 1).boxed().map(args::get).collect(toImmutableList()))
                .add("daemon");

        ImmutableList.Builder<String> shBuilder = ImmutableList.<String>builder()
                // .add("setsid")
                .addAll(builder.build().stream().map(s -> shellEscape(s)).collect(toImmutableList()));
        shBuilder.add("</dev/null");

        if (!isNullOrEmpty(config.getStdoutFile())) {
            shBuilder.add(">>" + shellEscape(replaceVars(config.getStdoutFile())));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        if (!isNullOrEmpty(config.getStderrFile())) {
            shBuilder.add("2>>" + shellEscape(replaceVars(config.getStderrFile())));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        shBuilder.add("&");

        String cmd = Joiner.on(" ").join(shBuilder.build());

        POSIX posix = POSIXUtils.getPOSIX();
        File sh = new File("/bin/sh");
        checkState(sh.exists() && sh.isFile());
        posix.libc().execv(sh.getAbsolutePath(), sh.getAbsolutePath(), "-c", cmd);
        throw new IllegalStateException("Unreachable");
    }
}
