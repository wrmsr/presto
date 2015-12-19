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
package com.wrmsr.presto.hadoop;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.hadoop.config.ConfigContainer;
import com.wrmsr.presto.hadoop.hive.HiveConfig;
import com.wrmsr.presto.hadoop.hive.HiveMetastore;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.util.config.PrestoConfigs;

import java.util.List;

public class HadoopPlugin
        implements Plugin, ServerEvent.Listener
{
    private final ConfigContainer config;

    public HadoopPlugin()
    {
        config = PrestoConfigs.loadConfigFromProperties(ConfigContainer.class);
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ServerEvent.Listener.class) {
            return ImmutableList.of(type.cast(this));
        }
        else {
            return ImmutableList.of();
        }
    }

    @Override
    public void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.ConnectorsLoaded) {
            // new HiveMetastore().start();
            HiveConfig hiveConfig = config.getMergedNode(HiveConfig.class);
            for (String name : hiveConfig.getStartMetastores()) {
                new HiveMetastore().start();
            }
        }
    }
}
