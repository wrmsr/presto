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
package com.wrmsr.presto.hardcoded;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

public class HardcodedModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorHandleResolver.class).to(HardcodedHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(HardcodedMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(HardcodedSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(HardcodedRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(HardcodedConnector.class).in(Scopes.SINGLETON);
        // configBinder(binder).bindConfig(HardcodedConfig.class);
    }
}
