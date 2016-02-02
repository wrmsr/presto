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
package com.wrmsr.presto.launcher.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.util.Serialization;
import io.airlift.airline.Cli;

import java.io.File;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Files.readAllBytesNoThrow;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class ClusterModule
        extends LauncherModule
{
    @Override
    public void configureCli(Cli.CliBuilder<Runnable> builder)
    {
        builder.withGroup("cluster")
                .withCommands()
    }

    @Override
    public ConfigContainer rewriteConfig(ConfigContainer config, Function<ConfigContainer, ConfigContainer> postprocess)
    {
        LauncherConfig launcherConfig = config.getMergedNode(LauncherConfig.class);
        ClustersConfig clustersConfig = config.getMergedNode(ClustersConfig.class);
        if (isNullOrEmpty(launcherConfig.getClusterNameFile())) {
            return config;
        }

        File clusterNameFile = new File(launcherConfig.getClusterNameFile());
        if (!clusterNameFile.exists()) {
            return config;
        }

        String clusterName = new String(readAllBytesNoThrow(clusterNameFile.toPath())).trim();
        ClusterConfig clusterConfig = clustersConfig.getEntries().get(clusterName);
        if (clusterConfig.getConfig() != null) {
            ConfigContainer updates = Serialization.roundTrip(OBJECT_MAPPER.get(), clusterConfig.getConfig(), ConfigContainer.class);
            config = (ConfigContainer) config.merge(updates);
            config = (ConfigContainer) config.merge(
                    Serialization.roundTrip(OBJECT_MAPPER.get(), ImmutableList.of(ImmutableMap.of("launcher", ImmutableMap.of("cluster-name", clusterName))), ConfigContainer.class));
            config = postprocess.apply(config);
        }

        if (isNullOrEmpty(launcherConfig.getClusterNodeNameFile())) {
            return config;
        }

        File clusterNodeNameFile = new File(launcherConfig.getClusterNodeNameFile());
        if (!clusterNodeNameFile.exists()) {
            return config;
        }

        String clusterNodeName = new String(readAllBytesNoThrow(clusterNodeNameFile.toPath())).trim();
        List clusterNodeConfig = ClusterUtils.getNodeConfig(clusterConfig, clusterNodeName);
        if (clusterNodeConfig != null) {
            ConfigContainer updates = Serialization.roundTrip(OBJECT_MAPPER.get(), clusterNodeConfig, ConfigContainer.class);
            config = (ConfigContainer) config.merge(updates);
            config = (ConfigContainer) config.merge(
                    Serialization.roundTrip(OBJECT_MAPPER.get(), ImmutableList.of(ImmutableMap.of("launcher", ImmutableMap.of("cluster-node-name", clusterNodeName))), ConfigContainer.class));
            config = postprocess.apply(config);
        }

        return config;
    }
}
