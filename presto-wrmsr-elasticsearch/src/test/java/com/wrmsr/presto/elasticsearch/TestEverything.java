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
package com.wrmsr.presto.elasticsearch;

import com.beust.jcommander.internal.Maps;
import com.wrmsr.presto.elasticsearch.util.ElasticsearchTestHelper;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.State;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class TestEverything
{
    protected final ElasticsearchTestHelper helper = new ElasticsearchTestHelper();

    @BeforeClass
    public void setElasticsearchDataDir() throws IOException
    {
        Path dataPath = Files.createTempDirectory(null);
    }

    @BeforeMethod
    public void resetDefaultSettings()
    {
        helper.resetDefaultSettings();
    }

    @AfterMethod
    public void closeNodes()
    {
        helper.closeAllNodes();
    }

    @Test
    public void testSingle()
            throws Exception
    {
        helper.startNode("server1");
        helper.waitForGreen("server1", "10s");
    }

    @Test
    public void testClusterSettingsTransmit() throws Exception
    {
        helper.startNode("server1");
        helper.waitForGreen("server1", "10s");

        /*
        helper.startNode("server2");
        helper.waitForGreen("server2", "10s");

        Map response = helper.httpClient("server1").request("_cluster/settings").response();
        assert ((Map) response.get("persistent")).size() == 0;
        response = helper.httpClient("server1").request("PUT", "_cluster/settings", "{\"persistent\": {\"cluster.blocks.read_only\": true}}".getBytes()).response();
        assert (Boolean) response.get("acknowledged") == true;

        response = helper.httpClient("server1").request("_cluster/settings").response();
        assert ((String) helper.getDeep(response, "persistent", "cluster", "blocks", "read_only")).equals("true");
        response = helper.httpClient("server2").request("_cluster/settings").response();
        assert ((String) helper.getDeep(response, "persistent", "cluster", "blocks", "read_only")).equals("true");

        helper.closeNode("server1");

        helper.startNode("server3");
        helper.waitForGreen("server3", "10s");

        response = helper.httpClient("server3").request("_cluster/settings").response();
        assert ((String) helper.getDeep(response, "persistent", "cluster", "blocks", "read_only")).equals("true");
        */

        InetSocketTransportAddress ista = ((InetSocketTransportAddress) helper.getHttpServerTransport("server1").boundAddress().publishAddress());
        int port = ista.address().getPort();
        JestClientFactory clientFactory = new JestClientFactory();
        clientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://localhost:" + port)
                        .build());
        JestClient client = clientFactory.getObject();
        JestResult res = client.execute(new State.Builder().build());
        System.out.println(res);

        /*
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put("number_of_shards",5);
        settingsBuilder.put("number_of_replicas",1);

        client.execute(new CreateIndex.Builder("messages").settings(settingsBuilder.build().getAsMap()).build());

        RootObjectMapper.Builder rootObjectMapperBuilder = new RootObjectMapper
                .Builder("message")
                .add(new StringFieldMapper.Builder("message").store(true));
        DocumentMapper documentMapper = new DocumentMapper
                .Builder("messages", null, rootObjectMapperBuilder)
                .build(null);
        String expectedMappingSource = documentMapper.mappingSource().toString();
        PutMapping putMapping = new PutMapping.Builder(
                "messages",
                "message",
                expectedMappingSource
        ).build();
        client.execute(putMapping);

        Map<String, String> source = Maps.newHashMap();
        source.put("message", "hi there");
        Index index = new Index.Builder(source).index("messges").type("messges").id("1").build();
        client.execute(index);
        */

        String settings = "\"settings\" : {\n" +
                "        \"number_of_shards\" : 5,\n" +
                "        \"number_of_replicas\" : 1\n" +
                "    }\n";

        client.execute(new CreateIndex.Builder("messages").settings(settings).build());

        PutMapping putMapping = new PutMapping.Builder(
                "messages",
                "message",
                "{ \"document\" : { \"properties\" : { \"message\" : {\"type\" : \"string\", \"store\" : \"yes\"} } } }"
        ).build();
        client.execute(putMapping);

        res = client.execute(new GetMapping.Builder().build());


        /*
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "myClusterName")
                .put("client.transport.sniff", true)
                .build();
        Client tclient = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        */
    }
}
