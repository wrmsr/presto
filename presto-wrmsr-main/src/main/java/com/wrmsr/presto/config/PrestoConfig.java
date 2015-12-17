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
package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.config.mergeable.MergeableConfig;

import java.io.IOException;
import java.util.Map;

public class PrestoConfig
        implements MergeableConfig<PrestoConfig>, Config<PrestoConfig>
{
    public PrestoConfig()
    {
    }

    @JsonDeserialize(using = AutoNodeId.Deserializer.class)
    public static abstract class AutoNodeId
    {
        public static class Deserializer
                extends StdDeserializer<AutoNodeId>
        {
            public Deserializer()
            {
                super(AutoNodeId.class);
            }

            @Override
            public AutoNodeId deserialize(JsonParser jp, DeserializationContext ctxt)
                    throws IOException
            {
                Object object = jp.readValueAs(Object.class);
                if (object instanceof String && ((String) object).equals("temp")) {
                    return new TempAutoNodeId();
                }
                else if (object instanceof Map) {
                    Map map = (Map) object;
                    String name = (String) Iterables.getOnlyElement(map.keySet());
                    Object value = Iterables.getOnlyElement(map.values());
                    if (name.equals("temp")) {
                        return new TempAutoNodeId();
                    }
                    else if (name.equals("file")) {
                        return new FileAutoNodeId((String) value);
                    }
                    else {
                        throw new IllegalArgumentException();
                    }
                }
                else {
                    throw new IllegalArgumentException();
                }
            }
        }
    }

    public static final class TempAutoNodeId
            extends AutoNodeId
    {
        @JsonValue
        public Object jsonValue()
        {
            return "temp";
        }
    }

    public static final class FileAutoNodeId
            extends AutoNodeId
    {
        private final String file;

        public FileAutoNodeId(String file)
        {
            this.file = file;
        }

        public String getFile()
        {
            return file;
        }

        @JsonValue
        public Object jsonValue()
        {
            return ImmutableMap.of("file", file);
        }
    }

    private AutoNodeId autoNodeId;

    @JsonProperty("auto-node-id")
    public AutoNodeId getAutoNodeId()
    {
        return autoNodeId;
    }

    @JsonProperty("auto-node-id")
    public void setAutoNodeId(AutoNodeId autoNodeId)
    {
        this.autoNodeId = autoNodeId;
    }
}
