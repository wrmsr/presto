package com.wrmsr.presto;

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Configs;
import com.wrmsr.presto.util.Files;
import com.wrmsr.presto.util.Serialization;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.testng.annotations.Test;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

public class TestConfig
{
    @Test
    public void testLoading() throws Throwable
    {
        String s;
        Map<String, String> p;

        s =
                "a=1\n" +
                "b=2\n" +
                "c=3\n";
        p = Configs.loadByExtension(s.getBytes(), "properties");
        System.out.println(p);

        s =
                "{\n" +
                        "\"number\": 2,\n" +
                        "\"single\": [3],\n" +
                        "\"things\": \"abc\",\n" +
                        "\"otherthings\": \"def\",\n" +
                        "\"deep\": {\n" +
                                "\"single\": [\"a\"],\n" +
                                "\"first\": \"a\",\n" +
                                "\"second\": \"b\"\n," +
                                "\"many\": [\"c\", \"d\", \"e\"]\n" +
                        "}\n" +
                "}\n";
        p = Configs.loadByExtension(s.getBytes(), "json");
        System.out.println(p);

        HierarchicalConfiguration hc;
        hc = Configs.toHierarchical(p);
        System.out.println(hc);

        System.out.println(newArrayList(hc.getKeys()));

        Map<String, String> p2;
        p2 = Configs.flattenValues(newHashMap(new ConfigurationMap(hc)));
        System.out.println(p2);

        HierarchicalConfiguration hc2;
        hc2 = Configs.toHierarchical(p2);
        System.out.println(hc2);

        Map<String, Object> unpacked = Configs.unpackHierarchical(hc2);
        System.out.println(unpacked);
    }

    @Test
    public void testThings() throws Throwable
    {
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
    }

    @Test
    public void testStuff() throws Throwable
    {
        String cfgStr = Files.readFile(System.getProperty("user.home") + "/presto/yelp-presto.yaml");
        Serialization.splitYaml(cfgStr).get(0);

        Object o = Serialization.YAML_OBJECT_MAPPER.get().readValue(cfgStr, Object.class);
        System.out.println(o);

        Map<String, Object> m = (Map<String, Object>) o;
        Map<String, String> t = Configs.flattenValues(o);

        Configuration c = new MapConfiguration(t);
        HierarchicalConfiguration hc = ConfigurationUtils.convertToHierarchical(c);
        System.out.println(hc);

        System.out.println(newHashMap(new ConfigurationMap(hc)));
    }

    @Test
    public void testFreemarker() throws Throwable
    {
        String templateSrc = "<html>\n" +
                "<head>\n" +
                "  <title>Welcome!</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "  <h1>Welcome ${user}!</h1>\n" +
                "  <p>Our latest product:\n" +
                "  <a href=\"${latestProduct.url}\">${latestProduct.name}</a>!\n" +
                "</body>\n" +
                "</html>  ";
       /* ------------------------------------------------------------------------ */
        /* You should do this ONLY ONCE in the whole application life-cycle:        */

        /* Create and adjust the configuration singleton */
        freemarker.template.Configuration cfg = new freemarker.template.Configuration(freemarker.template.Configuration.VERSION_2_3_22);
        cfg.setDirectoryForTemplateLoading(new File("/where/you/store/templates"));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        /* ------------------------------------------------------------------------ */
        /* You usually do these for MULTIPLE TIMES in the application life-cycle:   */

        /* Create a data-model */
        Map root = new HashMap();
        root.put("user", "Big Joe");
        Map latest = new HashMap();
        root.put("latestProduct", latest);
        latest.put("url", "products/greenmouse.html");
        latest.put("name", "green mouse");

        /* Get the template (uses cache internally) */
        Template temp = cfg.getTemplate("test.ftl");

        /* Merge data-model with template */
        Writer out = new OutputStreamWriter(System.out);
        temp.process(root, out);
        // Note: Depending on what `out` is, you may need to call `out.close()`.
        // This is usually the case for file output, but not for servlet output.
    }
}
