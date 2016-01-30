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
package com.wrmsr.presto.connector.views;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ViewsModule
    implements Module
{
    private final Class<? extends DirectoryViewStorage> storageCls;

    public ViewsModule(Class<? extends DirectoryViewStorage> storageCls)
    {
        this.storageCls = storageCls;
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ViewsConfig.class);

        binder.bind(ViewsConnector.class).asEagerSingleton();
        binder.bind(ViewsMetadata.class).asEagerSingleton();

        binder.bind(ViewStorage.class).to(storageCls);
    }

    @Provides
    @Singleton
    public DirectoryViewStorage provideDirectoryViewStorage(ViewsConfig config)
    {
        return new DirectoryViewStorage(config.getDirectory());
    }
}
