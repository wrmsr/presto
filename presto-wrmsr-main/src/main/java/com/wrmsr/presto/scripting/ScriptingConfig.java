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
package com.wrmsr.presto.scripting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.config.Config;
import com.wrmsr.presto.util.config.mergeable.MapMergeableConfig;

import javax.validation.constraints.NotNull;

import java.util.Map;

public class ScriptingConfig
        extends MapMergeableConfig<ScriptingConfig, String, ScriptingConfig.Entry>
        implements Config<ScriptingConfig>
{
    public static final class Entry
    {
        @NotNull
        private String engine;

        @JsonProperty("engine")
        public String getEngine()
        {
            return engine;
        }

        @JsonProperty("engine")
        public void setEngine(String engine)
        {
            this.engine = engine;
        }

        private boolean threadSafe;

        @JsonProperty("thread-safe")
        public boolean isThreadSafe()
        {
            return threadSafe;
        }

        @JsonProperty("thread-safe")
        public void setThreadSafe(boolean threadSafe)
        {
            this.threadSafe = threadSafe;
        }
    }

    @JsonCreator
    public ScriptingConfig(Map<String, Entry> entries)
    {
        super(entries);
    }

    public ScriptingConfig()
    {
        this(ImmutableMap.of());
    }

    @JsonValue
    @Override
    public Map<String, Entry> getEntries()
    {
        return super.getEntries();
    }
}
