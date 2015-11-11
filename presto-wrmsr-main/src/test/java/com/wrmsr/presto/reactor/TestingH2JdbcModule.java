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
package com.wrmsr.presto.reactor;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConfig;
import com.wrmsr.presto.jdbc.h2.H2Client;
import com.wrmsr.presto.jdbc.h2.H2ClientModule;
import org.h2.Driver;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

class TestingH2JdbcModule
        extends H2ClientModule
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }

    @Provides
    public JdbcClient provideJdbcClient(JdbcConnectorId id, BaseJdbcConfig config)
    {
        return new H2Client(id, config, new ExtendedJdbcConfig());
    }

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime()))
                .build();
    }

    public static Map<String, String> createProperties(File file)
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:%s;DB_CLOSE_DELAY=-1", file.getPath()))
                .build();
    }
}
