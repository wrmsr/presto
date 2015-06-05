package com.wrmsr.presto;

import com.facebook.presto.server.SliceSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.wrmsr.presto.util.Files;
import com.wrmsr.presto.util.ImmutableCollectors;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TestConfig
{
    private static final Supplier<ObjectMapper> OBJECT_MAPPER = Suppliers.memoize(() ->
            new ObjectMapperProvider().get()
                    .registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer())));

    @SuppressWarnings({"unchecked"})
    private Map<String, String> translate(String key, Object value)
    {
        if (value == null) {
            return ImmutableMap.of();
        }
        else if (value instanceof Map) {
            Map<String, Object> mapValue = (Map<String, Object>) value;
            return mapValue.entrySet().stream()
                    .flatMap(e -> translate(key + "." + e.getKey(), e.getValue()).entrySet().stream())
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else if (value instanceof List) {
            List<Object> listValue = (List<Object>) value;
            return IntStream.range(0, listValue.size()).boxed()
                    .flatMap(i -> translate(key + "(" + i.toString() + ")", listValue.get(i)).entrySet().stream())
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else {
            return ImmutableMap.of(key, value.toString());
        }
    }

    @Test
    public void testStuff() throws Throwable
    {
        String cfgStr = Files.readFile("/Users/wtimoney/yelp-presto.yaml");

        ObjectMapper omapper = OBJECT_MAPPER.get();
        ObjectMapper mapper = new ObjectMapper(
                new YAMLFactory(),
                (DefaultSerializerProvider) omapper.getSerializerProvider(),
                (DefaultDeserializationContext) omapper.getDeserializationContext()
        );
        Object o = mapper.readValue(cfgStr, Object.class);
        System.out.println(o);

        Map<String, Object> m = (Map<String, Object>) o;
        Map<String, String> t = translate("", o);

        Configuration c = new MapConfiguration(t);
        HierarchicalConfiguration hc = ConfigurationUtils.convertToHierarchical(c);
        System.out.println(hc);
    }
}
