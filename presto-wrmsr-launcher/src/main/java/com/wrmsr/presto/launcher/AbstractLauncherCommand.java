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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.Files.makeDirsAndCheck;
import static com.wrmsr.presto.util.Strings.splitProperty;
import static java.util.Objects.requireNonNull;

public abstract class AbstractLauncherCommand
        implements LauncherCommand
{
    private static final Logger log = Logger.get(AbstractLauncherCommand.class);

    // TODO bootstrap in run, member inject
    public static final ThreadLocal<String[]> ORIGINAL_ARGS = new ThreadLocal<>();

    @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
    public List<String> configFiles = new ArrayList<>();

    @Option(type = OptionType.GLOBAL, name = {"-C"}, description = "Set config item")
    public List<String> configItems = new ArrayList<>();

    @Option(type = OptionType.GLOBAL, name = {"-D"}, description = "Sets system property")
    public List<String> systemProperties = newArrayList();

    public AbstractLauncherCommand()
    {
    }

    private void setArgSystemProperties()
    {
        for (String prop : systemProperties) {
            String[] parts = splitProperty(prop);
            System.setProperty(parts[0], parts[1]);
        }
    }

    @GuardedBy("this")
    private Optional<LauncherModule> module = Optional.empty();

    @GuardedBy("this")
    private Optional<Injector> injector = Optional.empty();

    private volatile Optional<ConfigContainer> config = Optional.empty();

    @Override
    public synchronized void configure(LauncherModule module)
            throws Exception
    {
        requireNonNull(module);
        checkState(!this.module.isPresent());
        checkState(!this.injector.isPresent());
        checkState(!this.config.isPresent());
        this.module = Optional.of(module);

        for (String prop : configItems) {
            String[] parts = splitProperty(prop);
            PrestoConfigs.setConfigItem(parts[0], parts[1]);
        }
        ConfigContainer config = PrestoConfigs.loadConfig(getClass(), ConfigContainer.class, configFiles);
        config = module.postprocessConfig(config);
        config = module.rewriteConfig(config, module::postprocessConfig);
        PrestoConfigs.writeConfigProperties(config);
        this.config = Optional.of(config);

        Bootstrap app = new Bootstrap(new Module() {
            @Override
            public void configure(Binder binder)
            {
                AbstractLauncherCommand.this.module.get().configureLauncher(AbstractLauncherCommand.this.config.get(), binder);
            }
        });
        Injector injector = app
                .doNotInitializeLogging()
                .strictConfig()
                .initialize();
        try {
            for (String dir : firstNonNull(config.getMergedNode(LauncherConfig.class).getEnsureDirs(), ImmutableList.<String>of())) {
                makeDirsAndCheck(new File(dir));
            }

            injector.injectMembers(this);
        }
        finally {
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    @Override
    public synchronized void close()
            throws Exception
    {
        checkState(this.injector.isPresent());
        injector.get().getInstance(LifeCycleManager.class).stop();
    }

    public ConfigContainer getConfig()
    {
        return config.get();
    }
}
