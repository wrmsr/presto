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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wrmsr.presto.util.config.mergeable.StringMapMergeableConfig;

import java.util.List;
import java.util.Optional;

public final class JvmConfig
        extends StringMapMergeableConfig<JvmConfig>
        implements Config<JvmConfig>
{
    private boolean alreadyConfigured;

    @JsonProperty("already-configured")
    public boolean isAlreadyConfigured()
    {
        return alreadyConfigured;
    }

    @JsonProperty("already-configured")
    public void setAlreadyConfigured(boolean alreadyConfigured)
    {
        this.alreadyConfigured = alreadyConfigured;
    }

    private List<String> options;

    @JsonProperty
    public List<String> getOptions()
    {
        return options;
    }

    @JsonProperty
    public void setOptions(List<String> options)
    {
        this.options = options;
    }

    private Optional<Integer> debugPort = Optional.empty();

    @JsonProperty("debug-port")
    public Optional<Integer> getDebugPort()
    {
        return debugPort;
    }

    @JsonProperty("debug-port")
    public void setDebugPort(Optional<Integer> debugPort)
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

    private Optional<String> heap = Optional.empty();

    @JsonProperty("heap")
    public Optional<String> getHeap()
    {
        return heap;
    }

    @JsonProperty("heap")
    public void setHeap(Optional<String> heap)
    {
        this.heap = heap;
    }

    private Optional<String> gc = Optional.empty();

    @JsonProperty("gc")
    public Optional<String> getGc()
    {
        return gc;
    }

    @JsonProperty("gc")
    public void setGc(Optional<String> gc)
    {
        this.gc = gc;
    }
}
