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
import com.wrmsr.presto.util.config.mergeable.StringMapMergeableConfig;
import io.airlift.units.DataSize;

import java.math.BigDecimal;
import java.util.List;

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

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = FixedHeapSize.class, name = "fixed"),
            @JsonSubTypes.Type(value = PercentHeapSize.class, name = "percent"),
            @JsonSubTypes.Type(value = AutoHeapSize.class, name = "auto"),
    })
    public static abstract class HeapSize
    {
    }

    public static final class FixedHeapSize extends HeapSize
    {
        private DataSize value;

        @JsonProperty("value")
        public DataSize getValue()
        {
            return value;
        }

        @JsonProperty("value")
        public void setValue(DataSize value)
        {
            this.value = value;
        }
    }

    public static abstract class VariableHeapSize extends HeapSize
    {
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

    public static final class PercentHeapSize extends VariableHeapSize
    {
        private int value;

        @JsonProperty("value")
        public int getValue()
        {
            return value;
        }

        @JsonProperty("value")
        public void setValue(int value)
        {
            this.value = value;
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
    }

    public static final class AutoHeapSize extends VariableHeapSize
    {
    }

    private HeapSize heap;

    @JsonProperty("heap")
    public HeapSize getHeap()
    {
        return heap;
    }

    @JsonProperty("heap")
    public void setHeap(HeapSize heap)
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
}
