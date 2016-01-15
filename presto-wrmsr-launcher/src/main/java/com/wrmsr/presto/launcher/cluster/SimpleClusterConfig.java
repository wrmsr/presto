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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class SimpleClusterConfig
        extends ClusterConfig
{
    public static final class Node
    {
        private ShellRunner.Target target;
        private Path root;
        private int port;
        private List config = ImmutableList.of();

        public ShellRunner.Target getTarget()
        {
            return target;
        }

        @JsonProperty("target")
        public void setTarget(ShellRunner.Target target)
        {
            this.target = target;
        }

        @JsonProperty("root")
        public Path getRoot()
        {
            return root;
        }

        @JsonProperty("root")
        public void setRoot(Path root)
        {
            this.root = root;
        }

        @JsonProperty("port")
        public int getPort()
        {
            return port;
        }

        @JsonProperty("port")
        public void setPort(int port)
        {
            this.port = port;
        }

        @JsonProperty("config")
        public List getConfig()
        {
            return config;
        }

        @JsonProperty("config")
        public void setConfig(List config)
        {
            this.config = config;
        }
    }

    private Map defaults = ImmutableMap.of();

    @JsonProperty("defaults")
    public Map getDefaults()
    {
        return defaults;
    }

    @JsonProperty("defaults")
    public void setDefaults(Map defaults)
    {
        this.defaults = defaults;
    }

    private Map<String, Node> nodes = ImmutableMap.of();

    @JsonProperty("nodes")
    public Map<String, Node> getNodes()
    {
        return nodes;
    }

    @JsonProperty("nodes")
    public void setNodes(Map<String, Node> nodes)
    {
        this.nodes = nodes;
    }

    private String master;

    @JsonProperty("master")
    public String getMaster()
    {
        return master;
    }

    @JsonProperty("master")
    public void setMaster(String master)
    {
        this.master = master;
    }
}
