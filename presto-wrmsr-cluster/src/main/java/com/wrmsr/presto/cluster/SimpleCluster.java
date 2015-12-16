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
package com.wrmsr.presto.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.Box;
import io.airlift.units.DataSize;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class SimpleCluster
    implements Cluster
{
    public static class Config
    {
        public static class Node
        {
            public static class Name
                    extends Box<String>
            {
                @JsonCreator
                public Name(String value)
                {
                    super(value);
                }

                @JsonValue
                @Override
                public String getValue()
                {
                    return super.getValue();
                }
            }

            public static abstract class Size
            {

            }

            public static class DedicatedSize
                    extends Size
            {
            }

            public static class SharedSize
                    extends Size
            {
            }

            public static class ManualSize
                    extends Size
            {
                private final DataSize heap;

                public ManualSize(DataSize heap)
                {
                    this.heap = heap;
                }
            }

            protected final RemoteRunner.Target target;
            protected final Path root;
            protected final int port;
            protected final boolean isMaster;
            protected final Path data;
            protected final Size size;

            public Node(RemoteRunner.Target target, Path root, int port, boolean isMaster, Path data, Size size)
            {
                this.target = target;
                this.root = root;
                this.port = port;
                this.isMaster = isMaster;
                this.data = data;
                this.size = size;
            }

            public RemoteRunner.Target getTarget()
            {
                return target;
            }

            public Path getRoot()
            {
                return root;
            }

            public int getPort()
            {
                return port;
            }

            public boolean isMaster()
            {
                return isMaster;
            }

            public Path getData()
            {
                return data;
            }
        }

        protected final Optional<Map> nodeDefaults;
        protected final Map<Node.Name, Node> nodes;

        public Config(Optional<Map> nodeDefaults, Map<Node.Name, Node> nodes)
        {
            this.nodeDefaults = nodeDefaults;
            this.nodes = nodes;
        }
    }

    protected final Config config;
    protected final RemoteRunner remoteRunner;
    protected final Config.Node masterNode;

    public SimpleCluster(Config config, RemoteRunner remoteRunner, Config.Node masterNode)
    {
        this.config = config;
        this.remoteRunner = remoteRunner;
        this.masterNode = masterNode;
    }

    @Override
    public void bootstrap()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    protected String buildNodeUri(Config.Node node)
    {
        return String.format("http://%s:%d", node.getTarget().getHost(), node.getPort());
    }

    protected Map<String, String> buildNodeProperties(Config.Node.Name name, Config.Node node)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("http-server.http.port", Integer.toString(node.getPort()))
                .put("discovery.uri", buildNodeUri(masterNode))
                .put("node.data-dir", node.getData().toString());
        if (node.isMaster()) {
            return ImmutableMap.<String, String>builder()
                    .put("coordinator", "true")
                    .put("discovery-server.enabled", "true")
                    .build();
        }
        else {
            return ImmutableMap.<String, String>builder()
                    .put("coordinator", "false")
                    .build();
        }
    }
}
