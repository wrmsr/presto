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
package com.wrmsr.presto.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
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

public class Serialization
{
    private Serialization()
    {
    }

    // FIXME NOSTATIC
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

    public static final Supplier<ObjectMapper> XML_OBJECT_MAPPER = Suppliers.memoize(() -> {
        // ObjectMapper mapper = OBJECT_MAPPER.get();
        // return new XmlMapper(
        //     new XmlFactory(),
        //     (DefaultSerializerProvider) mapper.getSerializerProvider(),
        //     (DefaultDeserializationContext) mapper.getDeserializationContext());
        // FIXME guava stuff
        return new XmlMapper();
    });

    public static final ImmutableMap<String, Supplier<ObjectMapper>> OBJECT_MAPPERS_BY_EXTENSION = ImmutableMap.<String, Supplier<ObjectMapper>>builder()
            .put("json", JSON_OBJECT_MAPPER)
            .put("yaml", YAML_OBJECT_MAPPER)
            .put("yml", YAML_OBJECT_MAPPER)
            .put("xml", XML_OBJECT_MAPPER)
            .build();

    public static String toJsonString(Object object)
    {
        try {
            return Serialization.JSON_OBJECT_MAPPER.get().writerWithDefaultPrettyPrinter().writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
