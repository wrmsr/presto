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
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.server.daemon.DaemonModule;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ServerModule
        extends LauncherModule.Composite
{
    public ServerModule()
    {
        super(new DaemonModule());
    }

    @Override
    public void configureServerParent(ConfigContainer config, Binder binder)
    {
        newSetBinder(binder, ServerSystemPropertyProvider.class);
        newSetBinder(binder, ServerJvmArgumentProvider.class);

        binder.bind(SystemPropertyJvmArgumentForwarder.class).asEagerSingleton();
        newSetBinder(binder, ServerJvmArgumentProvider.class).addBinding().to(SystemPropertyJvmArgumentForwarder.class);
    }
}
