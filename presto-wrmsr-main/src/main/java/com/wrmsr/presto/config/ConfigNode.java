package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConnectorsConfigNode.class, name = "connectors"),
        @JsonSubTypes.Type(value = ExecConfigNode.class, name = "exec"),
        @JsonSubTypes.Type(value = IncludeConfigNode.class, name = "include"),
        @JsonSubTypes.Type(value = LogConfigNode.class, name = "log"),
        @JsonSubTypes.Type(value = PluginsConfigNode.class, name = "plugins"),
        @JsonSubTypes.Type(value = JvmConfigNode.class, name = "jvm"),
        @JsonSubTypes.Type(value = SystemConfigNode.class, name = "system"),
})
public abstract class ConfigNode
{
}
