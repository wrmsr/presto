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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.codec.Codec;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

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

    public static <T> T readPropertiesFile(File file, Class<T> cls)
    {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        ObjectMapper mapper = OBJECT_MAPPER.get();
        return roundTrip(mapper, props, cls);
    }

    public static <T> T readFile(File file, Class<T> cls)
    {
        String ext = com.google.common.io.Files.getFileExtension(file.getName());
        if (ext.equals("properties")) {
            return readPropertiesFile(file, cls);
        }
        else {
            try {
                byte[] cfgBytes = java.nio.file.Files.readAllBytes(file.toPath());
                ObjectMapper mapper = OBJECT_MAPPERS_BY_EXTENSION.get(ext).get();
                return mapper.readValue(cfgBytes, cls);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static String toJsonString(Object object)
    {
        try {
            return Serialization.JSON_OBJECT_MAPPER.get().writerWithDefaultPrettyPrinter().writeValueAsString(object);
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public static <T> Codec<T, byte[]> codecFor(Supplier<ObjectMapper> mapper, Class<T> cls)
    {
        return new Codec<T, byte[]>()
        {
            @Override
            public T decode(byte[] data)
            {
                try {
                    return mapper.get().readValue(data, cls);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] encode(T data)
            {
                try {
                    return mapper.get().writeValueAsBytes(data);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static <T> T roundTrip(ObjectMapper mapper, Object object, Class<T> cls)
    {
        try {
            return mapper.readValue(mapper.writeValueAsBytes(object), cls);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @SuppressWarnings({"unchecked"})
    public static <T> T roundTrip(ObjectMapper mapper, Object object, TypeReference valueTypeRef)
    {
        try {
            return (T) mapper.readValue(mapper.writeValueAsBytes(object), valueTypeRef);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static BiMap<String, Class<?>> getJsonSubtypeMap(ObjectMapper mapper, Class<?> cls)
    {
        AnnotationIntrospector ai = new JacksonAnnotationIntrospector();
        Annotated a = AnnotatedClass.construct(cls, ai, mapper.getDeserializationConfig());
        List<NamedType> nts = ai.findSubtypes(a);
        Map<String, Class<?>> m = nts.stream().map(nt -> ImmutablePair.<String, Class<?>>of(nt.getName(), nt.getType())).collect(toImmutableMap());
        return ImmutableBiMap.copyOf(m);
    }
}
