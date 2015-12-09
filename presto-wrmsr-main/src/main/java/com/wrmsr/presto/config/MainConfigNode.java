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
        @JsonSubTypes.Type(value = ConnectorsConfigNode.class, name = "connectors"),
        @JsonSubTypes.Type(value = ExecConfigNode.class, name = "exec"),
        @JsonSubTypes.Type(value = LogConfigNode.class, name = "log"),
        @JsonSubTypes.Type(value = PluginsConfigNode.class, name = "plugins"),
        @JsonSubTypes.Type(value = JvmConfigNode.class, name = "jvm"),
        @JsonSubTypes.Type(value = ScriptingConfigNode.class, name = "scripting"),
        @JsonSubTypes.Type(value = SystemConfigNode.class, name = "system"),
})
public interface MainConfigNode<N extends MainConfigNode<N>>
        extends MergingConfigNode<N>
{
}
