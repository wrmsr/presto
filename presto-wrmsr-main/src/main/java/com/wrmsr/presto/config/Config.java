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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wrmsr.presto.scripting.ScriptingConfig;
import com.wrmsr.presto.util.config.mergeable.MergeableConfig;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConnectorsConfig.class, name = "connectors"),
        @JsonSubTypes.Type(value = ExecConfig.class, name = "exec"),
        @JsonSubTypes.Type(value = PluginsConfig.class, name = "plugins"),
        @JsonSubTypes.Type(value = PrestoConfig.class, name = "presto"),
        @JsonSubTypes.Type(value = ScriptingConfig.class, name = "scripting"),
})
public interface Config<N extends Config<N>>
        extends MergeableConfig<N>
{
}
