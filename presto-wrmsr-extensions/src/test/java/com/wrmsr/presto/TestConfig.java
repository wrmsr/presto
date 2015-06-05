package com.wrmsr.presto;

import com.wrmsr.presto.util.Files;
import com.wrmsr.presto.util.Serialization;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.testng.annotations.Test;

import java.util.Map;

public class TestConfig
{
    @Test
    public void testStuff() throws Throwable
    {
        String cfgStr = Files.readFile(System.getProperty("user.home") + "/presto/yelp-presto.yaml");
        Serialization.splitYaml(cfgStr).get(0);

        Object o = Serialization.YAML_OBJECT_MAPPER.get().readValue(cfgStr, Object.class);
        System.out.println(o);

        Map<String, Object> m = (Map<String, Object>) o;
        Map<String, String> t = Serialization.flattenYaml(null, o);

        Configuration c = new MapConfiguration(t);
        HierarchicalConfiguration hc = ConfigurationUtils.convertToHierarchical(c);
        System.out.println(hc);
    }
}
