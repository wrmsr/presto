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
package com.wrmsr.presto.hive;

import com.facebook.presto.hive.HiveConnector;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.Connector;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupport;
import com.wrmsr.presto.spi.connectorSupport.ConnectorSupportFactory;

import java.util.Optional;

public class HiveConnectorSupportFactory
    implements ConnectorSupportFactory
{
    @Override
    public <T extends ConnectorSupport> Optional<T> getConnectorSupport(Class<T> cls, ConnectorSession cs, Connector c)
    {
        if (c instanceof HiveConnector && cls.isAssignableFrom(HiveConnectorSupport.class)) {
            return Optional.of((T) new HiveConnectorSupport(cs, (HiveConnector) c));
        }
        else {
            return Optional.empty();
        }
    }
}
