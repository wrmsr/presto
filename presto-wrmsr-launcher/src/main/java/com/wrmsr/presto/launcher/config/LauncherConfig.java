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

import java.util.Optional;

public class LauncherConfig
        implements Config<LauncherConfig>
{
    public LauncherConfig()
    {
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

    private Integer debutPort;

    @JsonProperty("debug-port")
    public Integer getDebutPort()
    {
        return debutPort;
    }

    @JsonProperty("debug-port")
    public void setDebutPort(Integer debutPort)
    {
        this.debutPort = debutPort;
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
}
