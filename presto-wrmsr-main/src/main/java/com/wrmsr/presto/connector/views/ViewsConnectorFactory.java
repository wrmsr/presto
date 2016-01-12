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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;

import javax.inject.Inject;

import java.io.File;
import java.util.Map;

public class ViewsConnectorFactory
        implements ConnectorFactory
{
    private final ViewAnalyzer viewAnalyzer;

    @Inject
    public ViewsConnectorFactory(ViewAnalyzer viewAnalyzer)
    {
        this.viewAnalyzer = viewAnalyzer;
    }

    @Override
    public String getName()
    {
        return "views";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ViewsHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config)
    {
        File directory = new File(config.get("directory"));
        return new ViewsConnector(new ViewsConnectorMetadata(new DirectoryViewStorage(directory), viewAnalyzer));
    }
}
