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
package com.wrmsr.presto.launcher.jvm;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class JvmModule
        extends LauncherModule
{
    @Override
    public void configureLauncher(ConfigContainer config, Binder binder)
    {
        binder.bind(JvmManager.class).in(Scopes.SINGLETON);

        newSetBinder(binder, JvmOptionProvider.class);

        newSetBinder(binder, JvmOptionProvider.class).addBinding().to(DebugJvmOptionProvider.class).asEagerSingleton();
        newSetBinder(binder, JvmOptionProvider.class).addBinding().to(GcJvmOptionProvider.class).asEagerSingleton();
        newSetBinder(binder, JvmOptionProvider.class).addBinding().to(HeapJvmOptionProvider.class).asEagerSingleton();
        newSetBinder(binder, JvmOptionProvider.class).addBinding().to(RepositoryJvmOptionProvider.class).asEagerSingleton();
    }
}
