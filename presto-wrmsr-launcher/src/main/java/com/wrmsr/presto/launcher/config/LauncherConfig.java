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
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class LauncherConfig
        implements Config<LauncherConfig>
{
    public LauncherConfig()
    {
    }

    private List<String> ensureDirs;

    @JsonProperty("ensure-dirs")
    public List<String> getEnsureDirs()
    {
        return ensureDirs;
    }

    @JsonProperty("ensure-dirs")
    public void setEnsureDirs(List<String> ensureDirs)
    {
        this.ensureDirs = ensureDirs;
    }

    private String pidFile;

    @JsonProperty("pid-file")
    public String getPidFile()
    {
        return pidFile;
    }

    @JsonProperty("pid-file")
    public void setPidFile(String pidFile)
    {
        this.pidFile = pidFile;
    }

    private String logFile;

    @JsonProperty("log-file")
    public String getLogFile()
    {
        return logFile;
    }

    @JsonProperty("log-file")
    public void setLogFile(String logFile)
    {
        this.logFile = logFile;
    }

    private String clusterName;

    @JsonProperty("cluster-name")
    public String getClusterName()
    {
        return clusterName;
    }

    @JsonProperty("cluster-name")
    public void setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
    }

    private String clusterNameFile;

    @JsonProperty("cluster-name-file")
    public String getClusterNameFile()
    {
        return clusterNameFile;
    }

    @JsonProperty("cluster-name-file")
    public void setClusterNameFile(String clusterNameFile)
    {
        this.clusterNameFile = clusterNameFile;
    }

    private String clusterNodeNameFile;

    @JsonProperty("cluster-node-name-file")
    public String getClusterNodeNameFile()
    {
        return clusterNodeNameFile;
    }

    @JsonProperty("cluster-node-name-file")
    public void setClusterNodeNameFile(String clusterNodeNameFile)
    {
        this.clusterNodeNameFile = clusterNodeNameFile;
    }

    private String clusterNodeName;

    @JsonProperty("cluster-node-name")
    public String getClusterNodeName()
    {
        return clusterNodeName;
    }

    @JsonProperty("cluster-node-name")
    public void setClusterNodeName(String clusterNodeName)
    {
        this.clusterNodeName = clusterNodeName;
    }
}
