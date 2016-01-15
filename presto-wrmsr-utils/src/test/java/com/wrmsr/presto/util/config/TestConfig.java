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
package com.wrmsr.presto.util.config;

import com.wrmsr.presto.util.Files;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.Configs;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.TreeMap;

public class TestConfig
{
    @Test
    public void testLoading()
            throws Throwable
    {
        /*
        Configs.ListPreservingDefaultExpressionEngine engine = new Configs.ListPreservingDefaultExpressionEngine();
        Configs.ListPreservingDefaultConfigurationKey key = new Configs.ListPreservingDefaultConfigurationKey(engine,
                // "x.y.z"
                "hi(0).(1).there(3).(4).x"
                // "hi(0,1).there(3,4).x"
        );
        Configs.ListPreservingDefaultConfigurationKey.KeyIterator it = key.iterator();
        while (it.hasNext()) {
            it.next();
        }
        */
        String s;
        Map<String, String> p;

        s = "" +
                "a=1\n" +
                "b=2\n" +
                "c=3\n";
        p = Configs.loadByExtension(s.getBytes(), "properties");
        // System.out.println(p);

        s = "" +
                "{\n" +
                "\"number\": 2,\n" +
                "\"many\": [\"c\", \"d\", \"e\"],\n" +
                "\"single\": [3],\n" +
                "\"abc\":{\"fml\": [[3]]},\n" +
                // "\"things\": \"abc\",\n" +
                // "\"otherthings\": \"def\",\n" +
                "\"deep\": {\n" +
                "\"single\": [\"a\"],\n" +
                "\"first\": \"a\",\n" +
                "\"second\": \"b\",\n" +
                "\"many\": [\"c\", \"d\", \"e\"],\n" +
                "\"abc\":{\"fml\": [[[[{\"hi\":\"there\"}]]]]}\n" +
                "}\n" +
                "}\n";
        p = Configs.loadByExtension(s.getBytes(), "json");
        // System.out.println(p);

        HierarchicalConfiguration hc;

        // hc = ConfigurationUtils.convertToHierarchical(new MapConfiguration(p), new XPathExpressionEngine());
        // System.out.println(hc);

        /*
        HierarchicalConfiguration config = hc;
        config.addProperty("tables table/name", "tasks");
        config.addProperty("tables/table[last()] @type", "system");
        config.addProperty("tables/table[last()] fields/field/name", "taskid");
        config.addProperty("tables/table[last()]/fields/field[last()] type", "int");
        config.addProperty("tables/table[last()]/fields field/name", "name");
        config.addProperty("tables/table[last()]/fields field/name", "startDate");
        */

        /*
        hc = Configs.toHierarchical(p);
        // System.out.println(hc);

        // System.out.println(newArrayList(hc.getKeys()));

        Map<String, String> p2;
        Map<String, Object> mx = Configs.unpackHierarchical(hc);
        p2 = Configs.flatten(mx);
        // System.out.println(p2);

        HierarchicalConfiguration hc2;
        hc2 = Configs.toHierarchical(p2);
        // System.out.println(hc2);

        Map<String, Object> unpacked = Configs.unpackHierarchical(hc2);
        // System.out.println(unpacked);
        */

        Object o;

        for (int i = 0; i < 3; ++i) {
            hc = Configs.CONFIG_PROPERTIES_CODEC.decode(p);
            o = Configs.OBJECT_CONFIG_CODEC.decode(hc);
            System.out.println(o);

            hc = Configs.OBJECT_CONFIG_CODEC.encode(o);
            p = Configs.CONFIG_PROPERTIES_CODEC.encode(hc);
            System.out.println(p);
        }
    }

    @Test
    public void testThings()
            throws Throwable
    {
        /*
        Map<String, String> strs;
        HierarchicalConfiguration hc;

        strs = ImmutableMap.of(
                "stuff.init", "abcd"
        );
        hc = Configs.toHierarchical(strs).configurationAt("stuff");
        System.out.println(Configs.getAllStrings(hc, "init"));

        strs = ImmutableMap.of(
                "stuff.init(0)", "abcd",
                "stuff.init(1)", "defg"
        );
        hc = Configs.toHierarchical(strs).configurationAt("stuff");
        System.out.println(Configs.getAllStrings(hc, "init"));

        strs = ImmutableMap.of(
                "stuff.init.first", "abcd",
                "stuff.init.second", "defg"
        );
        hc = Configs.toHierarchical(strs).configurationAt("stuff");
        System.out.println(Configs.getAllStrings(hc, "init"));

        strs = ImmutableMap.of(
                "stuff.init.first", "abcd",
                "stuff.init.second", "defg",
                "stuff.other", "yeah"
        );
        hc = Configs.toHierarchical(strs).configurationAt("stuff");
        System.out.println(Configs.getAllStrings(hc, "init"));
        */
    }

    @Test
    public void testStuff()
            throws Throwable
    {
        String cfgStr = Files.readFile(System.getProperty("user.home") + "/presto/yelp-presto.yaml");
        Serialization.splitYaml(cfgStr).get(0);

        Object o = Serialization.YAML_OBJECT_MAPPER.get().readValue(cfgStr, Object.class);
        System.out.println(o);

        Map<String, Object> m = (Map<String, Object>) o;
        /*
        Map<String, String> t = Configs.flatten(o);

        Configuration c = new MapConfiguration(t);
        HierarchicalConfiguration hc = ConfigurationUtils.convertToHierarchical(c);
        System.out.println(hc);

        System.out.println(newHashMap(new ConfigurationMap(hc)));
        */
    }

//    @Test
//    public void testFreemarker() throws Throwable
//    {
//        String templateSrc = ""+
//                "<html>\n" +
//                "<head>\n" +
//                "  <title>Welcome!</title>\n" +
//                "</head>\n" +
//                "<body>\n" +
//                "  <h1>Welcome ${user}!</h1>\n" +
//                "  <p>Our latest product:\n" +
//                "  <a href=\"${latestProduct.url}\">${latestProduct.name}</a>!\n" +
//                "</body>\n" +
//                "</html>  ";
//       /* ------------------------------------------------------------------------ */
//        /* You should do this ONLY ONCE in the whole application life-cycle:        */
//
//        /* Create and adjust the configuration singleton */
//        freemarker.template.Configuration cfg = new freemarker.template.Configuration(freemarker.template.Configuration.VERSION_2_3_22);
//        cfg.setDirectoryForTemplateLoading(new File("/where/you/store/templates"));
//        cfg.setDefaultEncoding("UTF-8");
//        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
//
//        /* ------------------------------------------------------------------------ */
//        /* You usually do these for MULTIPLE TIMES in the application life-cycle:   */
//
//        /* Create a data-model */
//        Map root = new HashMap();
//        root.put("user", "Big Joe");
//        Map latest = new HashMap();
//        root.put("latestProduct", latest);
//        latest.put("url", "products/greenmouse.html");
//        latest.put("name", "green mouse");
//
//        /* Get the template (uses cache internally) */
//        Template temp = cfg.getTemplate("test.ftl");
//
//        /* Merge data-model with template */
//        Writer out = new OutputStreamWriter(System.out);
//        temp.process(root, out);
//        // Note: Depending on what `out` is, you may need to call `out.close()`.
//        // This is usually the case for file output, but not for servlet output.
//    }

    @Test
    public void testFuckYou()
            throws Throwable
    {
        Map<String, String> map = new TreeMap<>();

//        map.put("(0).jvm.thing", "true");
//        map.put("(1).system.h2.implicitRelativePath", "true");
//        map.put("(1).system.thing", "true");
//        map.put("(2).log.com.facebook.presto.server.PluginManager", "DEBUG");
//        map.put("(2).log.com.ning.http.client", "WARN");
//        map.put("(2).log.com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory", "WARN");

        HierarchicalConfiguration hierarchicalConfig = Configs.CONFIG_PROPERTIES_CODEC.decode(map);
        Object obj = Configs.OBJECT_CONFIG_CODEC.decode(hierarchicalConfig);
        System.out.println(obj);
    }

    @Test
    public void testUgh()
            throws Throwable
    {
        Configs.Sigil.parse("thing.other(1).fuck");
    }
}
