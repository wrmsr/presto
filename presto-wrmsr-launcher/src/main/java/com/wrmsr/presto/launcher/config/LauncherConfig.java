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

import java.util.List;

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

    public static final String PID_FILE_FD_KEY = "pid-file-fd";

    private Integer pidFileFd;

    @JsonProperty(PID_FILE_FD_KEY)
    public Integer getPidFileFd()
    {
        return pidFileFd;
    }

    @JsonProperty(PID_FILE_FD_KEY)
    public void setPidFileFd(Integer pidFileFd)
    {
        this.pidFileFd = pidFileFd;
    }

    private String serverLogFile;

    @JsonProperty("server-log-file")
    public String getServerLogFile()
    {
        return serverLogFile;
    }

    @JsonProperty("server-log-file")
    public void setServerLogFile(String serverLogFile)
    {
        this.serverLogFile = serverLogFile;
    }

    private String httpLogFile;

    @JsonProperty("http-log-file")
    public String getHttpLogFile()
    {
        return httpLogFile;
    }

    @JsonProperty("http-log-file")
    public void setHttpLogFile(String httpLogFile)
    {
        this.httpLogFile = httpLogFile;
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
