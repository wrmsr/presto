package com.wrmsr.presto.elasticsearch;

import com.wrmsr.presto.elasticsearch.util.ElasticsearchTestHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

public class TestEverything
{
    protected final ElasticsearchTestHelper helper = new ElasticsearchTestHelper();

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
    public void testClusterSettingsTransmit() throws Exception {
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
