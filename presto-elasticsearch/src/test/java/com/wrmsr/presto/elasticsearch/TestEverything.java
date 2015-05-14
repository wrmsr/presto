package com.wrmsr.presto.elasticsearch;

import com.wrmsr.presto.elasticsearch.util.ElasticsearchTestHelper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
}
