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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.mergeable.MapMergeableConfig;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Map;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableMap;

public final class ConnectorsConfig
        extends MapMergeableConfig<ConnectorsConfig, String, ConnectorsConfig.Entry>
        implements Config<ConnectorsConfig>
{
    public static final class Entry
    {
        @JsonCreator
        public static Entry valueOf(Object object)
        {
            return new Entry(Configs.flatten(object));
        }

        protected final Map<String, String> entries;

        public Entry(Map<String, String> entries)
        {
            this.entries = ImmutableMap.copyOf(entries);
        }

        @JsonValue
        public Map<String, String> getEntries()
        {
            return entries;
        }
    }

    @JsonCreator
    public ConnectorsConfig(Map<String, Entry> entries)
    {
        super(entries);
    }

    @JsonValue
    public Map<String, Map<String, String>> flatten()
    {
        return getEntries().entrySet().stream().map(e -> ImmutablePair.of(e.getKey(), e.getValue().getEntries())).collect(toImmutableMap());
    }
}
