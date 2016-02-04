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
package com.wrmsr.presto.launcher.server;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.LaunchCommand;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import io.airlift.airline.Cli;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ServerModule
        extends LauncherModule
{
    @Override
    public void configureCli(Cli.CliBuilder<Runnable> builder)
    {
        builder.withCommands(
                LaunchCommand.class,
                RunCommand.class);
    }

    @Override
    public void configureLauncher(ConfigContainer config, Binder binder)
    {
        binder.bind(ServerManager.class).in(Scopes.SINGLETON);

        newSetBinder(binder, ServerPropertyProvider.class);
    }
}
