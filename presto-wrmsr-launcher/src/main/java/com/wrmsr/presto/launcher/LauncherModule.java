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

    public void configureLauncher(ConfigContainer config, Binder binder)
    {
    }

    public void configureServerLogging(ConfigContainer config)
    {
    }

    public void configureServer(ConfigContainer config, Binder binder)
    {
    }

    public static class Composite
            extends LauncherModule
    {
        private final List<LauncherModule> children;

        public Composite(Iterable<LauncherModule> children)
        {
            this.children = ImmutableList.copyOf(children);
        }

        public Composite(LauncherModule first, LauncherModule... rest)
        {
            this(ImmutableList.<LauncherModule>builder().add(first).add(rest).build());
        }

        @Override
        public final List<Class<? extends LauncherCommand>> getLauncherCommands()
        {
            ImmutableList.Builder<Class<? extends LauncherCommand>> builder = ImmutableList.builder();
            builder.addAll(getLauncherCommandsParent());
            for (LauncherModule child : children) {
                builder.addAll(child.getLauncherCommands());
            }
            return builder.build();
        }

        @Override
        public final ConfigContainer rewriteConfig(ConfigContainer config)
        {
            config = rewriteConfigParent(config);
            for (LauncherModule child : children) {
                config = child.rewriteConfig(config);
            }
            return config;
        }

        @Override
        public final void configureLauncher(ConfigContainer config, Binder binder)
        {
            configureLauncherParent(config, binder);
            for (LauncherModule child : children) {
                child.configureLauncher(config, binder);
            }
        }

        @Override
        public void configureServerLogging(ConfigContainer config)
        {
            configureServerLoggingParent(config);
            for (LauncherModule child : children) {
                child.configureServerLogging(config);
            }
        }

        @Override
        public final void configureServer(ConfigContainer config, Binder binder)
        {
            configureServerParent(config, binder);
            for (LauncherModule child : children) {
                child.configureServer(config, binder);
            }
        }

        public List<Class<? extends LauncherCommand>> getLauncherCommandsParent()
        {
            return ImmutableList.of();
        }

        public ConfigContainer rewriteConfigParent(ConfigContainer config)
        {
            return config;
        }

        public void configureLauncherParent(ConfigContainer config, Binder binder)
        {
        }

        public void configureServerLoggingParent(ConfigContainer config)
        {
        }

        public void configureServerParent(ConfigContainer config, Binder binder)
        {
        }
    }
}
