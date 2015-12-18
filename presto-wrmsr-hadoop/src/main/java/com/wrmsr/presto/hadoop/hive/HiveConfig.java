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
package com.wrmsr.presto.hadoop.hive;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.hadoop.config.Config;

import java.util.List;
import java.util.Map;

public class HiveConfig
    implements Config<HiveConfig>
{
    public static class Metastore
    {
    }

    private Map<String, Metastore> metastores = ImmutableMap.of();

    @JsonProperty("metastores")
    public Map<String, Metastore> getMetastores()
    {
        return metastores;
    }

    @JsonProperty("metastores")
    public void setMetastores(Map<String, Metastore> metastores)
    {
        this.metastores = metastores;
    }

    private List<String> startMetastores = ImmutableList.of();

    @JsonProperty("start-metastores")
    public List<String> getStartMetastores()
    {
        return startMetastores;
    }

    @JsonProperty("start-metastores")
    public void setStartMetastores(List<String> startMetastores)
    {
        this.startMetastores = startMetastores;
    }
}
