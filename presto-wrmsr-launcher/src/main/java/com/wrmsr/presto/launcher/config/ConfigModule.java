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
package com.wrmsr.presto.launcher.config;

import com.google.inject.Binder;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Serialization;
import com.wrmsr.presto.util.config.PrestoConfigs;
import jnr.posix.POSIX;

import java.util.Map;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.Strings.replaceStringVars;
import static com.wrmsr.presto.util.collect.Maps.transformValues;

public class ConfigModule
        extends LauncherModule
{
    @Override
    public void configureLauncher(ConfigContainer config, Binder binder)
    {
        for (Class cls : Serialization.getJsonSubtypeMap(OBJECT_MAPPER.get(), Config.class).values()) {
            binder.bind(cls).toInstance(config.getMergedNode(cls));
        }
    }

    @Override
    public ConfigContainer postprocessConfig(ConfigContainer config)
    {
        setConfigEnv(config);
        setConfigSystemProperties(config);
        return PrestoConfigs.configFromProperties(transformValues(PrestoConfigs.configToProperties(config), this::replaceVars), ConfigContainer.class);
    }

    private volatile POSIX posix;

    public synchronized POSIX getPosix()
    {
        if (posix == null) {
            posix = POSIXUtils.getPOSIX();
        }
        return posix;
    }

    private String getEnvOrSystemProperty(String key)
    {
        String value = getPosix().getenv(key.toUpperCase());
        if (value != null) {
            return value;
        }
        return System.getProperty(key);
    }

    private String replaceVars(String text)
    {
        return replaceStringVars(text, this::getEnvOrSystemProperty);
    }

    private void setConfigEnv(ConfigContainer config)
    {
        for (EnvConfig c : config.getNodes(EnvConfig.class)) {
            for (Map.Entry<String, String> e : c) {
                getPosix().libc().setenv(e.getKey(), replaceVars(e.getValue()), 1);
            }
        }
    }

    private void setConfigSystemProperties(ConfigContainer config)
    {
        for (SystemConfig c : config.getNodes(SystemConfig.class)) {
            for (Map.Entry<String, String> e : c) {
                System.setProperty(e.getKey(), replaceVars(e.getValue()));
            }
        }
    }
}

