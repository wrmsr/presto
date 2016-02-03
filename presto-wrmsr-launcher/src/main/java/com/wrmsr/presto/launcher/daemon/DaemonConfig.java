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
package com.wrmsr.presto.launcher.daemon;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.wrmsr.presto.launcher.config.Config;

public class DaemonConfig
        implements Config<DaemonConfig>
{
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
    @JsonPropertyDescription("INTERNAL")
    public void setPidFileFd(Integer pidFileFd)
    {
        this.pidFileFd = pidFileFd;
    }

    private String stdoutFile;

    @JsonProperty("stdout-file")
    public String getStdoutFile()
    {
        return stdoutFile;
    }

    @JsonProperty("stdout-file")
    public void setStdoutFile(String stdoutFile)
    {
        this.stdoutFile = stdoutFile;
    }

    private String stderrFile;

    @JsonProperty("stderr-file")
    public String getStderrFile()
    {
        return stderrFile;
    }

    @JsonProperty("stderr-file")
    public void setStderrFile(String stderrFile)
    {
        this.stderrFile = stderrFile;
    }
}
