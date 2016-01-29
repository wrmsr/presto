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
import com.wrmsr.presto.util.Serialization;

import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;

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
}
