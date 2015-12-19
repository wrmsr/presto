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
package com.wrmsr.presto.launcher.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.launcher.cluster.SimpleClusterConfig;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.mergeable.MergeableConfig;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class JvmConfig
        implements Config<JvmConfig>
{
    public JvmConfig()
    {
    }

    public static final String ALREADY_CONFIGURED_KEY = "already-configured";

    private boolean alreadyConfigured;

    @JsonProperty(ALREADY_CONFIGURED_KEY)
    public boolean isAlreadyConfigured()
    {
        return alreadyConfigured;
    }

    @JsonProperty(ALREADY_CONFIGURED_KEY)
    public void setAlreadyConfigured(boolean alreadyConfigured)
    {
        this.alreadyConfigured = alreadyConfigured;
    }

    private Integer debugPort;

    @JsonProperty("debug-port")
    public Integer getDebugPort()
    {
        return debugPort;
    }

    @JsonProperty("debug-port")
    public void setDebugPort(Integer debugPort)
    {
        this.debugPort = debugPort;
    }

    private boolean debugSuspend;

    @JsonProperty("debug-suspend")
    public boolean isDebugSuspend()
    {
        return debugSuspend;
    }

    @JsonProperty("debug-suspend")
    public void setDebugSuspend(boolean debugSuspend)
    {
        this.debugSuspend = debugSuspend;
    }

    public static final class HeapConfig
    {
        private String size;

        @JsonProperty("size")
        public String getSize()
        {
            return size;
        }

        @JsonProperty("size")
        public void setSize(String size)
        {
            this.size = size;
        }

        private DataSize min;

        @JsonProperty("min")
        public DataSize getMin()
        {
            return min;
        }

        @JsonProperty("min")
        public void setMin(DataSize min)
        {
            this.min = min;
        }

        private DataSize max;

        @JsonProperty("max")
        public DataSize getMax()
        {
            return max;
        }

        @JsonProperty("max")
        public void setMax(DataSize max)
        {
            this.max = max;
        }

        private boolean free;

        @JsonProperty("free")
        public boolean isFree()
        {
            return free;
        }

        @JsonProperty("free")
        public void setFree(boolean free)
        {
            this.free = free;
        }

        private boolean attempt;

        @JsonProperty("attempt")
        public boolean isAttempt()
        {
            return attempt;
        }

        @JsonProperty("attempt")
        public void setAttempt(boolean attempt)
        {
            this.attempt = attempt;
        }
    }

    private HeapConfig heap;

    @JsonProperty("heap")
    public HeapConfig getHeap()
    {
        return heap;
    }

    @JsonProperty("heap")
    public void setHeap(HeapConfig heap)
    {
        this.heap = heap;
    }
//
//    public static class GcConfigDeserializer
//            extends StdDeserializer<GcConfig>
//            implements ResolvableDeserializer
//    {
//        private static final long serialVersionUID = 7923585097068641765L;
//
//        private final JsonDeserializer<?> defaultDeserializer;
//
//        public GcConfigDeserializer(JsonDeserializer<?> defaultDeserializer)
//        {
//            super(GcConfig.class);
//            this.defaultDeserializer = defaultDeserializer;
//        }
//
//        @Override
//        public GcConfig deserialize(JsonParser jp, DeserializationContext ctxt)
//                throws IOException
//        {
//            GcConfig deserializedGcConfig = (GcConfig) defaultDeserializer.deserialize(jp, ctxt);
//
//            // Special logic
//
//            return deserializedGcConfig;
//        }
//
//        // for some reason you have to implement ResolvableDeserializer when modifying BeanDeserializer
//        // otherwise deserializing throws JsonMappingException??
//        @Override
//        public void resolve(DeserializationContext ctxt)
//                throws JsonMappingException
//        {
//            ((ResolvableDeserializer) defaultDeserializer).resolve(ctxt);
//        }
//    }
//
//    public static void main(String[] args)
//            throws Throwable
//    {
//        SimpleModule module = new SimpleModule();
//        module.setDeserializerModifier(new BeanDeserializerModifier()
//        {
//            @Override
//            public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer)
//            {
//                if (beanDesc.getBeanClass() == GcConfig.class) {
//                    return new GcConfigDeserializer(deserializer);
//                }
//                return deserializer;
//            }
//        });
//
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.registerModule(module);
//        GcConfig user = mapper.readValue("\"g1\"", GcConfig.class);
//    }


    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CmsGcConfig.class, name = "cms"),
            @JsonSubTypes.Type(value = G1GcConfig.class, name = "g1"),
    })
    public static abstract class GcConfig
    {
//
//        @JsonCreator
//        public static GcConfig valueOf(Object object)
//        {
//            if (object instanceof String) {
//                object = ImmutableMap.of((String) object, ImmutableMap.of());
//            }
//            Map map = (Map) object;
//            checkArgument(map.size() == 1);
//            ObjectMapper mapper = OBJECT_MAPPER.get();
//            Map<String, Class<?>> nodeTypeMap = Serialization.getJsonSubtypeMap(mapper, GcConfig.class);
//            Class<?> cls = nodeTypeMap.get(Iterables.getOnlyElement(map.keySet()));
//            return Serialization.roundTrip(mapper, map, cls);
//        }

        private boolean debug;

        @JsonProperty("debug")
        public boolean isDebug()
        {
            return debug;
        }

        @JsonProperty("debug")
        public void setDebug(boolean debug)
        {
            this.debug = debug;
        }
    }

    public static final class CmsGcConfig extends GcConfig
    {
    }

    public static final class G1GcConfig extends GcConfig
    {
        private long maxPauseMillis;

        @JsonProperty("max-pause-millis")
        public long getMaxPauseMillis()
        {
            return maxPauseMillis;
        }

        @JsonProperty("max-pause-millis")
        public void setMaxPauseMillis(long maxPauseMillis)
        {
            this.maxPauseMillis = maxPauseMillis;
        }
    }

    private GcConfig gc;

    @JsonProperty("gc")
    public GcConfig getGc()
    {
        return gc;
    }

    @JsonProperty("gc")
    public void setGc(GcConfig gc)
    {
        this.gc = gc;
    }

    private List<String> options = ImmutableList.of();

    @JsonProperty
    public List<String> getOptions()
    {
        return options;
    }

    @JsonProperty
    public void setOptions(List<String> options)
    {
        this.options = checkNotNull(options);
    }
}
