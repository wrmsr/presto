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
package com.wrmsr.presto.launcher.commands;

import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.util.DaemonProcess;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public abstract class AbstractDaemonCommand
        extends AbstractLauncherCommand
{
    private DaemonProcess daemonProcess;

    private String pidFile()
    {
        return getConfig().getMergedNode(LauncherConfig.class).getPidFile();
    }

    public synchronized DaemonProcess getDaemonProcess()
    {
        if (daemonProcess == null) {
            checkArgument(!isNullOrEmpty(pidFile()), "must set pidfile");
            daemonProcess = new DaemonProcess(new File(replaceVars(pidFile())), getConfig().getMergedNode(LauncherConfig.class).getPidFileFd());
        }
        return daemonProcess;
    }
}
