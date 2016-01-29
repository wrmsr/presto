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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.cluster.ClusterModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import com.wrmsr.presto.launcher.config.ConfigModule;
import com.wrmsr.presto.launcher.leadership.LeadershipModule;
import com.wrmsr.presto.launcher.logging.LoggingModule;
import com.wrmsr.presto.launcher.passthrough.PassthroughModule;
import com.wrmsr.presto.launcher.server.ServerModule;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.launcher.zookeeper.ZookeeperModule;
import jnr.posix.POSIX;

public class MainLauncherModule
        extends LauncherModule.Composite
{
    public MainLauncherModule()
    {
        super(
                new ClusterModule(),
                new ConfigModule(),
                new LeadershipModule(),
                new LoggingModule(),
                new PassthroughModule(),
                new ServerModule(),
                new ZookeeperModule());
    }

    @Override
    public void configureLauncherParent(ConfigContainer config, Binder binder)
    {
        binder.bind(POSIX.class).toProvider(POSIXUtils::getPOSIX).in(Scopes.SINGLETON);
    }
}
