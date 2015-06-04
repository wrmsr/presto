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
package com.wrmsr.presto.jdbc;

import com.facebook.presto.plugin.jdbc.JdbcConnectorFactory;
import com.facebook.presto.plugin.jdbc.JdbcRecordSetProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;

import java.util.List;
import java.util.Map;

public class ExtendedJdbcConnectorFactory
        extends JdbcConnectorFactory
{
    public ExtendedJdbcConnectorFactory(String name, Module module, Map<String, String> optionalConfig, ClassLoader classLoader)
    {
        super(name, module, optionalConfig, classLoader);
    }

    @Override
    protected List<Module> createModules(String connectorId)
    {
        return ImmutableList.of(Modules.override(super.createModules(connectorId)).with(new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(JdbcRecordSetProvider.class).to(ExtendedJdbcRecordSetProvider.class).in(Scopes.SINGLETON);
            }
        }));
    }
}
