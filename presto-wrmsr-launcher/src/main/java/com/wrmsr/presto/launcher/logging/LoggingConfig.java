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
package com.wrmsr.presto.launcher.logging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.config.Config;
import com.wrmsr.presto.util.config.mergeable.MergeableConfig;

import java.util.List;
import java.util.Map;

public final class LoggingConfig
        implements MergeableConfig<LoggingConfig>, Config<LoggingConfig>
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = SubprocessAppenderConfig.class, name = "subprocess"),
    })
    public static abstract class AppenderConfig
    {
    }

    public static final class SubprocessAppenderConfig
            extends AppenderConfig
    {

    }

    @JsonCreator
    public LoggingConfig()
    {
    }

    private List<AppenderConfig> appenders = ImmutableList.of();

    @JsonProperty("appenders")
    public List<AppenderConfig> getAppenders()
    {
        return appenders;
    }

    @JsonProperty("appenders")
    public void setAppenders(List<AppenderConfig> appenders)
    {
        this.appenders = ImmutableList.copyOf(appenders);
    }

    private Map<String, String> levels = ImmutableMap.of();

    @JsonProperty("levels")
    public Map<String, String> getLevels()
    {
        return levels;
    }

    @JsonProperty("levels")
    public void setLevels(Map<String, String> levels)
    {
        this.levels = ImmutableMap.copyOf(levels);
    }

    private String configFile;

    @JsonProperty("config-file")
    public String getConfigFile()
    {
        return configFile;
    }

    @JsonProperty("config-file")
    public void setConfigFile(String configFile)
    {
        this.configFile = configFile;
    }

    private String configXml;

    @JsonProperty("config-xml")
    public String getConfigXml()
    {
        return configXml;
    }

    @JsonProperty("config-xml")
    public void setConfigXml(String configXml)
    {
        this.configXml = configXml;
    }
}
