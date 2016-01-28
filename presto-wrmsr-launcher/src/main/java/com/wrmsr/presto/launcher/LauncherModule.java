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
package com.wrmsr.presto.launcher;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.wrmsr.presto.launcher.commands.LauncherCommand;
import com.wrmsr.presto.launcher.config.ConfigContainer;

import java.util.List;

public abstract class LauncherModule
{
    public List<Class<? extends LauncherCommand>> getLauncherCommands()
    {
        return ImmutableList.of();
    }

    public ConfigContainer rewriteConfig(ConfigContainer config)
    {
        return config;
    }

    public void configureLauncher(Binder binder)
    {
    }

    public void configureServer(Binder binder)
    {
    }

    public static final class Composite
        extends LauncherModule
    {
        private final List<LauncherModule> children;

        public Composite(Iterable<LauncherModule> children)
        {
            this.children = ImmutableList.copyOf(children);
        }

        @Override
        public List<Class<? extends LauncherCommand>> getLauncherCommands()
        {
            ImmutableList.Builder<Class<? extends LauncherCommand>> builder = ImmutableList.builder();
            for (LauncherModule child : children) {
                builder.addAll(child.getLauncherCommands());
            }
            return builder.build();
        }

        @Override
        public ConfigContainer rewriteConfig(ConfigContainer config)
        {
            for (LauncherModule child : children) {
                config = child.rewriteConfig(config);
            }
            return config;
        }

        @Override
        public void configureLauncher(Binder binder)
        {
            for (LauncherModule child : children) {
                child.configureLauncher(binder);
            }
        }

        @Override
        public void configureServer(Binder binder)
        {
            for (LauncherModule child : children) {
                child.configureServer(binder);
            }
        }
    }
}
