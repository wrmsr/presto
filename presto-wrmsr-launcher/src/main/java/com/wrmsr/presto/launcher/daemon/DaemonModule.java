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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import io.airlift.airline.Cli;

public class DaemonModule
        extends LauncherModule
{
    @Override
    public void configureCli(Cli.CliBuilder<Runnable> builder)
    {
        builder.withCommands(
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

//        binder.install(new GuiceUtils.EmptyModule()
//        {
//            public DaemonProcess get()
//            {
//                checkArgument(!isNullOrEmpty(pidFile()), "must set pidfile");
//                return new DaemonProcess(new File(replaceVars(pidFile())), getConfig().getMergedNode(LauncherConfig.class).getPidFileFd());
//            }
//        });
    }
}
