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
package com.wrmsr.presto.connector;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.wrmsr.presto.connector.jdbc.h2.H2ConnectorFactory;
import com.wrmsr.presto.connector.jdbc.mysql.ExtendedMySqlConnectorFactory;
import com.wrmsr.presto.connector.jdbc.postgresql.ExtendedPostgreSqlConnectorFactory;
import com.wrmsr.presto.connector.jdbc.redshift.RedshiftConnectorFactory;
import com.wrmsr.presto.connector.jdbc.sqlite.SqliteConnectorFactory;
import com.wrmsr.presto.connector.jdbc.temp.TempConnectorFactory;
import com.wrmsr.presto.connector.partitioner.PartitionerMetaconnectorFactory;
import com.wrmsr.presto.connector.views.ViewAnalyzer;
import com.wrmsr.presto.connector.views.ViewsConnectorFactory;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class ConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ViewAnalyzer.class).asEagerSingleton();
        binder.bind(ViewsConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, ConnectorFactory.class).addBinding().to(ViewsConnectorFactory.class);

        binder.bind(H2ConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(H2ConnectorFactory.class);

        binder.bind(ExtendedMySqlConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(ExtendedMySqlConnectorFactory.class);

        binder.bind(ExtendedPostgreSqlConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(ExtendedPostgreSqlConnectorFactory.class);

        binder.bind(RedshiftConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(RedshiftConnectorFactory.class);

        binder.bind(SqliteConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(SqliteConnectorFactory.class);

        binder.bind(TempConnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, com.facebook.presto.spi.ConnectorFactory.class).addBinding().to(TempConnectorFactory.class);

        binder.bind(MetaconnectorManager.class).asEagerSingleton();

        binder.bind(PartitionerMetaconnectorFactory.class).asEagerSingleton();
        newSetBinder(binder, MetaconnectorFactory.class).addBinding().to(PartitionerMetaconnectorFactory.class);
    }
}
