package com.wrmsr.presto.launcher.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wrmsr.presto.util.config.merging.MergingConfigNode;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class LauncherConfig
    implements MergingConfigNode<LauncherConfig>
{
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
                    return new TempNodeId();
                }
                else if (object instanceof Map) {
                    Map map = (Map) object;
                    String name = (String) Iterables.getOnlyElement(map.keySet());
                    Object value = Iterables.getOnlyElement(map.values());
                    if (name.equals("temp")) {
                        return new TempNodeId();
                    }
                    else if (name.equals("file")) {
                        return new FileNodeId((String) value);
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

    public static final class TempNodeId
            extends AutoNodeId
    {
        @JsonValue
        public Object jsonValue()
        {
            return "temp";
        }
    }

    public static final class FileNodeId
            extends AutoNodeId
    {
        private final String file;

        public FileNodeId(String file)
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

    private Optional<AutoNodeId> autoNodeId = Optional.empty();

    @JsonProperty("auto-node-id")
    public Optional<AutoNodeId> getAutoNodeId()
    {
        return autoNodeId;
    }

    @JsonProperty("auto-node-id")
    public void setAutoNodeId(Optional<AutoNodeId> autoNodeId)
    {
        this.autoNodeId = autoNodeId;
    }

    private Optional<String> pidFile = Optional.empty();

    @JsonProperty("pid-file")
    public Optional<String> getPidFile()
    {
        return pidFile;
    }

    @JsonProperty("pid-file")
    public void setPidFile(Optional<String> pidFile)
    {
        this.pidFile = pidFile;
    }

    private Optional<Integer> debutPort = Optional.empty();

    @JsonProperty("debug-port")
    public Optional<Integer> getDebutPort()
    {
        return debutPort;
    }

    @JsonProperty("debug-port")
    public void setDebutPort(Optional<Integer> debutPort)
    {
        this.debutPort = debutPort;
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
}
