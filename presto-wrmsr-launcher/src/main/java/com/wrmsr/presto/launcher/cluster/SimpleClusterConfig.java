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

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class SimpleClusterConfig
{
    public static class Node
    {
        private RemoteRunner.Target target;
        private Path root;
        private int port;
        private boolean master;
        private Path data;
        private Map config;

        @JsonProperty("target")
        public RemoteRunner.Target getTarget()
        {
            return target;
        }

        @JsonProperty("target")
        public void setTarget(RemoteRunner.Target target)
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

        @JsonProperty("master")
        public boolean isMaster()
        {
            return master;
        }

        @JsonProperty("master")
        public void setMaster(boolean master)
        {
            this.master = master;
        }

        @JsonProperty("data")
        public Path getData()
        {
            return data;
        }

        @JsonProperty("data")
        public void setData(Path data)
        {
            this.data = data;
        }

        @JsonProperty("config")
        public Map getConfig()
        {
            return config;
        }

        @JsonProperty("config")
        public void setConfig(Map config)
        {
            this.config = config;
        }
    }

    private Map defaults;

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

    private Map<String, Node> nodes;

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
}
