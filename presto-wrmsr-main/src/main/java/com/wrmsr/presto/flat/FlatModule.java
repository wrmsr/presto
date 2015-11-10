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
package com.wrmsr.presto.flat;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class FlatModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ConnectorHandleResolver.class).to(FlatHandleResolver.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(FlatMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(FlatSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(FlatRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSinkProvider.class).to(FlatRecordSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(FlatConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(FlatConfig.class);
    }
}
