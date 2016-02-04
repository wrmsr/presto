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
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Inject;
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    @Option(type = OptionType.GLOBAL, name = {"-D"}, description = "Sets system property")
    public List<String> systemProperties = newArrayList();

    @Option(type = OptionType.GLOBAL, name = {"-C"}, description = "Set config item")
    public List<String> configItems = new ArrayList<>();

    @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
    public List<String> configFiles = new ArrayList<>();

    public AbstractLauncherCommand()
    {
    }

    private final AtomicBoolean isConfigured = new AtomicBoolean();

    @Override
    public synchronized void configure(LauncherModule module, List<String> args)
            throws Exception
    {
        requireNonNull(module);
        checkState(!isConfigured.get());

        for (String prop : systemProperties) {
            String[] parts = splitProperty(prop);
            System.setProperty(parts[0], parts[1]);
        }
        for (String prop : configItems) {
            String[] parts = splitProperty(prop);
            PrestoConfigs.setConfigItem(parts[0], parts[1]);
        }
        final ConfigContainer config;
        {
            ConfigContainer config_ = PrestoConfigs.loadConfig(getClass(), ConfigContainer.class, configFiles);
            config_ = module.postprocessConfig(config_);
            config_ = module.rewriteConfig(config_, module::postprocessConfig);
            config = config_;
        }
        PrestoConfigs.writeConfigProperties(PrestoConfigs.configToProperties(config));

        for (String dir : firstNonNull(config.getMergedNode(LauncherConfig.class).getEnsureDirs(), ImmutableList.<String>of())) {
            makeDirsAndCheck(new File(dir));
        }

        Bootstrap app = new Bootstrap(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(LauncherCommand.Args.class).toInstance(new Args(args));
                module.configureLauncher(config, binder);
            }
        });
        Injector injector = app
                .doNotInitializeLogging()
                .strictConfig()
                .initialize();

        injector.injectMembers(this);
        isConfigured.compareAndSet(false, true);
    }

    @Inject
    private LifeCycleManager lifeCycleManager;

    @Override
    public synchronized void close()
            throws Exception
    {
        if (isConfigured.get()) {
            requireNonNull(lifeCycleManager);
            lifeCycleManager.stop();
        }
    }
}
