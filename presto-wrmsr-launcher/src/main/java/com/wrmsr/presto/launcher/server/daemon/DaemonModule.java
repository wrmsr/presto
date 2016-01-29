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
package com.wrmsr.presto.launcher.server.daemon;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.LauncherCommand;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.util.GuiceUtils;

import java.io.File;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class DaemonModule
        extends LauncherModule
{
    @Override
    public List<Class<? extends LauncherCommand>> getLauncherCommands()
    {
        return ImmutableList.<Class<? extends LauncherCommand>>of(
                KillCommand.class,
                RestartCommand.class,
                StartCommand.class,
                StatusCommand.class,
                StopCommand.class);
    }

    @Override
    public void configureServer(ConfigContainer config, Binder binder)
    {
        binder.bind(DaemonManager.class).in(Scopes.SINGLETON);

        binder.install(new GuiceUtils.EmptyModule()
        {
            public DaemonProcess get()
            {
                checkArgument(!isNullOrEmpty(pidFile()), "must set pidfile");
                return new DaemonProcess(new File(replaceVars(pidFile())), getConfig().getMergedNode(LauncherConfig.class).getPidFileFd());
            }
        });
    }
}
