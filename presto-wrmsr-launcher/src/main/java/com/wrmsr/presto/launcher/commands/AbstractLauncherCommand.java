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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.cluster.ClusterConfig;
import com.wrmsr.presto.launcher.cluster.ClusterUtils;
import com.wrmsr.presto.launcher.cluster.ClustersConfig;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.EnvConfig;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.config.SystemConfig;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.log.Logger;
import jnr.posix.POSIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Files.makeDirsAndCheck;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.Strings.replaceStringVars;
import static com.wrmsr.presto.util.Strings.splitProperty;

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

    public static File getJvm()
    {
        File jvm = new File(System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : ""));
        checkState(jvm.exists() && jvm.isFile());
        return jvm;
    }

    @Override
    public final void run()
    {
        setArgSystemProperties();
        getConfig();
        ensureConfigDirs();

        try {
            launcherRun();
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }
    }

    public abstract void launcherRun()
            throws Throwable;
}
