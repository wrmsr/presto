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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.config.mergeable.StringMapMergeableConfig;
import io.airlift.units.DataSize;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class JvmConfig
        extends StringMapMergeableConfig<JvmConfig>
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

    private String gc;

    @JsonProperty("gc")
    public String getGc()
    {
        return gc;
    }

    @JsonProperty("gc")
    public void setGc(String gc)
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
