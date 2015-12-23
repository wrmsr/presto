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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.hadoop.config.ConfigContainer;
import com.wrmsr.presto.hadoop.hive.HiveConfig;
import com.wrmsr.presto.hadoop.hive.HiveMetastoreService;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.log.Logger;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class HadoopPlugin
        implements Plugin, ServerEvent.Listener
{
    private static final Logger log = Logger.get(HadoopPlugin.class);

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
            HiveConfig hiveConfig = config.getMergedNode(HiveConfig.class);
            for (Map.Entry<String, HiveConfig.Metastore> e : hiveConfig.getMetastores().entrySet()) {
                HiveConfig.Metastore metastore = e.getValue();
                if (metastore.isRun()) {
                    Map<String, String> properties = new HashMap<>();
                    if (metastore.getDb() instanceof HiveConfig.Metastore.LocalDb) {
                        HiveConfig.Metastore.LocalDb localDb = (HiveConfig.Metastore.LocalDb) metastore.getDb();
                        checkArgument(!Strings.isNullOrEmpty(localDb.getFile()));
                        properties.put("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
                        properties.put("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", URLEncoder.encode(localDb.getFile())));
                    }
                    else {
                        log.warn("no metastore db configured");
                    }

                    HadoopService svc = new HiveMetastoreService(properties, new String[0]);
                    svc.start();
                }
            }
        }
    }
}
