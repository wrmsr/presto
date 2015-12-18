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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SimpleCluster
    implements Cluster
{

    protected final SimpleClusterConfig config;
    protected final RemoteRunner remoteRunner;
    protected final SimpleClusterConfig.Node masterNode;

    public SimpleCluster(SimpleClusterConfig config, RemoteRunner remoteRunner)
    {
        this.config = config;
        this.remoteRunner = remoteRunner;
        masterNode = config.getNodes().get(checkNotNull(config.getMaster()));
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

    protected String buildNodeUri(SimpleClusterConfig.Node node)
    {
        return String.format("http://%s:%d", node.getTarget().getHost(), node.getPort());
    }

    protected Map<String, String> buildNodeProperties(String name, SimpleClusterConfig.Node node)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("http-server.http.port", Integer.toString(node.getPort()))
                .put("discovery.uri", buildNodeUri(masterNode))
                .put("node.data-dir", node.getData().toString());
        if (name.equals(config.getMaster())) {
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
