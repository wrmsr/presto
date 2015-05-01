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
package com.wrmsr.presto.elasticsearch.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;

import java.io.File;
import java.util.Map;
import java.util.UUID;

public class ElasticsearchTestHelper
{
    protected final ESLogger logger = Loggers.getLogger(getClass());

    protected Map<String, Node> nodes = Maps.newHashMap();

    protected Map<String, Client> clients = Maps.newHashMap();

    protected Settings defaultSettings = ImmutableSettings.settingsBuilder()
            .put("node.local", true)
            .put("gateway.type", "none")
            .put("index.store.type", "memory")
            .put("network.host", "127.0.0.1")
            .build();

    public void resetDefaultSettings()
    {
        putDefaultSettings(ImmutableSettings.builder()
                .put("cluster.name", "test-cluster-" + UUID.randomUUID()));
    }

    public Settings defaultSettings()
    {
        return defaultSettings;
    }

    private boolean deleteDirectory(File path)
    {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                }
                else {
                    if (!file.delete()) {
                        return false;
                    }
                }
            }
            return path.delete();
        }
        return false;
    }

    public void putDefaultSettings(Settings.Builder settings)
    {
        putDefaultSettings(settings.build());
    }

    public void putDefaultSettings(Settings settings)
    {
        defaultSettings = ImmutableSettings.settingsBuilder().put(defaultSettings).put(settings).build();
    }

    public Node startNode(String id)
    {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings.Builder settings)
    {
        return startNode(id, settings.build());
    }

    public Node startNode(String id, Settings settings)
    {
        return buildNode(id, settings).start();
    }

    public Node buildNode(String id)
    {
        return buildNode(id, ImmutableSettings.EMPTY);
    }

    public Node buildNode(String id, Settings.Builder settings)
    {
        return buildNode(id, settings.build());
    }

    public Node buildNode(String id, Settings settings)
    {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = ImmutableSettings.settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(defaultSettings)
                .put(settings)
                .put("name", id)
                .build();

        if (finalSettings.get("gateway.type") == null) {
            // default to non gateway
            finalSettings = ImmutableSettings.settingsBuilder().put(finalSettings).put("gateway.type", "none").build();
        }

        Node node = NodeBuilder.nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public Node node(String id)
    {
        return nodes.get(id);
    }

    public HttpClient httpClient(String id)
    {
        return new HttpClient(getHttpServerTransport(id).boundAddress().publishAddress());
    }

    public HttpClient httpClient(String id, String username, String password)
    {
        return new HttpClient(getHttpServerTransport(id).boundAddress().publishAddress(), username, password);
    }

    public void closeNode(String id)
    {
        clients.get(id).close();
        clients.remove(id);
        nodes.get(id).close();
        nodes.remove(id);
    }

    public void closeAllNodes()
    {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }

    public HttpServerTransport getHttpServerTransport(String id)
    {
        return ((InternalNode) node(id)).injector().getInstance(HttpServerTransport.class);
    }

    public Map<String, Object> createSearchQuery(String queryString)
    {
        return MapBuilder.<String, Object>newMapBuilder()
                .put("query", MapBuilder.newMapBuilder()
                                .put("query_string", MapBuilder.newMapBuilder()
                                                .put("query", queryString)
                                                .immutableMap()
                                ).immutableMap()
                ).immutableMap();
    }

    public Client client(String id)
    {
        return clients.get(id);
    }

    public void createTestIndex(String nodeId)
    {
        try {
            client(nodeId).admin().indices().prepareDelete("test").execute().actionGet();
        }
        catch (Exception e) {
        }
        client(nodeId).admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("number_of_shards", 1)
                                .put("number_of_replicas", 0))
                .execute().actionGet();
        client(nodeId).admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
    }

    public void waitForGreen(String id, String timeout) throws Exception
    {
        client(id).admin().cluster().prepareHealth().setTimeout(timeout).execute().actionGet();
    }

    public <T> T retryFor(long millis, long sleepMillis, Function<Void, T> fn)
    {
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                return fn.apply(null);
            }
            catch (Exception e) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= millis) {
                    throw e;
                }
                logger.warn("Polling", e);
                try {
                    Thread.sleep(sleepMillis);
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public <T> T retryFor(long millis, Function<Void, T> fn)
    {
        return retryFor(millis, 100, fn);
    }

    public Object getDeep(Map map, String... keys)
    {
        Object ret = map;
        for (String key : keys) {
            ret = ((Map) ret).get(key);
        }
        return ret;
    }

    public Object getDeepSafe(Map map, String... keys)
    {
        Object ret = map;
        for (String key : keys) {
            if (ret == null) {
                break;
            }
            ret = ((Map) ret).get(key);
        }
        return ret;
    }
}
