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

import com.wrmsr.presto.elasticsearch.util.ElasticsearchTestHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;

public class TestEverything
{
    protected final ElasticsearchTestHelper helper = new ElasticsearchTestHelper();

    @BeforeClass
    public void setElasticsearchDataDir() throws IOException
    {
        Files.createTempDirectory(null);

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
    }
}
