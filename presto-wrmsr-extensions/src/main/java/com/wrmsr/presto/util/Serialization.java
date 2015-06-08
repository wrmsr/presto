package com.wrmsr.presto.util;

import com.facebook.presto.server.SliceSerializer;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
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

    public static final Supplier<ObjectMapper> JSON_OBJECT_MAPPER = OBJECT_MAPPER;

    public static ObjectMapper forkObjectMapper(ObjectMapper mapper, JsonFactory jf)
    {
       return new ObjectMapper(
                jf,
                (DefaultSerializerProvider) mapper.getSerializerProvider(),
                (DefaultDeserializationContext) mapper.getDeserializationContext());
    }

    public static final Supplier<ObjectMapper> YAML_OBJECT_MAPPER = Suppliers.memoize(() -> forkObjectMapper(OBJECT_MAPPER.get(), new YAMLFactory()));

    public static final ThreadLocal<Yaml> YAML = ThreadLocal.withInitial(Yaml::new);

    public static List<Object> splitYaml(String src)
    {
        return ImmutableList.copyOf(YAML.get().loadAll(new ByteArrayInputStream(src.getBytes())));
    }

    public static final Supplier<ObjectMapper> XML_OBJECT_MAPPER = Suppliers.memoize(() -> forkObjectMapper(OBJECT_MAPPER.get(), new XmlFactory()));

    public static final ImmutableMap<String, Supplier<ObjectMapper>> OBJECT_MAPPERS_BY_EXTENSION = ImmutableMap.<String, Supplier<ObjectMapper>>builder()
            .put("json", JSON_OBJECT_MAPPER)
            .put("yaml", YAML_OBJECT_MAPPER)
            .put("yml", YAML_OBJECT_MAPPER)
            .put("xml", XML_OBJECT_MAPPER)
            .build();
}
