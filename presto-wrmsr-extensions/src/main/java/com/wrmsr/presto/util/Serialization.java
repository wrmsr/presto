package com.wrmsr.presto.util;

import com.facebook.presto.server.SliceSerializer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.Maps.newHashMap;

public class Serialization
{
    private Serialization()
    {
    }

    public static final Supplier<ObjectMapper> OBJECT_MAPPER = Suppliers.memoize(() ->
            new ObjectMapperProvider().get()
                    .registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer())));

    public static ObjectMapper forkObjectMapper(ObjectMapper mapper, JsonFactory jf)
    {
       return new ObjectMapper(
                jf,
                (DefaultSerializerProvider) mapper.getSerializerProvider(),
                (DefaultDeserializationContext) mapper.getDeserializationContext());
    }

    public static final Supplier<ObjectMapper> YAML_OBJECT_MAPPER = Suppliers.memoize(() -> forkObjectMapper(OBJECT_MAPPER.get(), new YAMLFactory()));

    public static final ThreadLocal<Yaml> YAML = ThreadLocal.withInitial(Yaml::new);

    @SuppressWarnings({"unchecked"})
    public static Map<String, String> flattenYaml(String key, Object value)
    {
        /*
        TODO:
        !!binary	byte[], String
        !!timestamp	java.util.Date, java.sql.Date, java.sql.Timestamp
        !!omap, !!pairs	List of Object[]
        */
        if (value == null) {
            Map<String, String> map = newHashMap();
            map.put(key, null);
            return map;
        }
        else if (value instanceof Map) {
            Map<String, Object> mapValue = (Map<String, Object>) value;
            return mapValue.entrySet().stream()
                    .flatMap(e -> flattenYaml((key != null ? key + "." : "") + e.getKey(), e.getValue()).entrySet().stream())
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else if (value instanceof List || value instanceof Set) {
            List<Object> listValue = ImmutableList.copyOf((Iterable<Object>) value);
            return IntStream.range(0, listValue.size()).boxed()
                    .flatMap(i -> flattenYaml(key + "(" + i.toString() + ")", listValue.get(i)).entrySet().stream())
                    .collect(ImmutableCollectors.toImmutableMap(e -> e.getKey(), e -> e.getValue()));
        }
        else {
            return ImmutableMap.of(key, value.toString());
        }
    }

    public static Map<String, String> flattenYaml(Object value)
    {
        return flattenYaml(null, value);
    }

    public static List<Object> splitYaml(String src)
    {
        return ImmutableList.copyOf(YAML.get().loadAll(new ByteArrayInputStream(src.getBytes())));
    }

    public static String renderYaml(Object obj)
    {
        return YAML.get().dump(obj);
    }
}
