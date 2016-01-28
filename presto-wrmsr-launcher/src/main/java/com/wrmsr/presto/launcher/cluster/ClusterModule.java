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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.util.Serialization;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

public class ClusterModule
        extends LauncherModule
{
    @Override
    public ConfigContainer rewriteConfig(ConfigContainer config)
    {
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
    }
}
