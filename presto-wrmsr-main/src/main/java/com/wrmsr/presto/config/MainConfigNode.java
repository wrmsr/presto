package com.wrmsr.presto.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wrmsr.presto.util.config.merging.MergingConfigNode;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT
        // defaultImpl = UnknownConfigNode.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConnectorsConfig.class, name = "connectors"),
        @JsonSubTypes.Type(value = ExecConfig.class, name = "exec"),
        @JsonSubTypes.Type(value = LogConfig.class, name = "log"),
        @JsonSubTypes.Type(value = PluginsConfig.class, name = "plugins"),
        @JsonSubTypes.Type(value = JvmConfig.class, name = "jvm"),
        @JsonSubTypes.Type(value = ScriptingConfig.class, name = "scripting"),
        @JsonSubTypes.Type(value = SystemConfig.class, name = "system"),
})
public interface MainConfigNode<N extends MainConfigNode<N>>
        extends MergingConfigNode<N>
{
}
