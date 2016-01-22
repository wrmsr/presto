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
package com.wrmsr.presto.launcher.zookeeper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wrmsr.presto.launcher.config.Config;

public class ZookeeperConfig
        implements Config<ZookeeperConfig>
{
    // TODO conn timeout, session timeout
    // PathUtils.validatePath(value);

    public ZookeeperConfig()
    {
    }

    private String connectionString;

    @JsonProperty("connection-string")
    public String getConnectionString()
    {
        return connectionString;
    }

    @JsonProperty("connection-string")
    public void setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
    }

    private String rootPath;

    @JsonProperty("root-path")
    public String getRootPath()
    {
        return rootPath;
    }

    @JsonProperty("root-path")
    public void setRootPath(String rootPath)
    {
        this.rootPath = rootPath;
    }

    private String masterMutexPath;

    @JsonProperty("master-mutex-path")
    public String getMasterMutexPath()
    {
        return masterMutexPath;
    }

    @JsonProperty("master-mutex-path")
    public void setMasterMutexPath(String masterMutexPath)
    {
        this.masterMutexPath = masterMutexPath;
    }

    private boolean masterMutexSuicide;

    @JsonProperty("master-mutex-suicide")
    public boolean isMasterMutexSuicide()
    {
        return masterMutexSuicide;
    }

    @JsonProperty("master-mutex-suicide")
    public void setMasterMutexSuicide(boolean masterMutexSuicide)
    {
        this.masterMutexSuicide = masterMutexSuicide;
    }
}
