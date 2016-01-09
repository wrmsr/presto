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
package com.wrmsr.presto.connector.partitioner;

import com.facebook.presto.spi.connector.ConnectorFactory;
import com.wrmsr.presto.connector.MetaconnectorFactory;

import java.util.Map;

public class PartitionerMetaconnectorFactory
    implements MetaconnectorFactory
{
    @Override
    public String getName()
    {
        return "partitioner";
    }

    @Override
    public ConnectorFactory create(String connectorName, Map<String, String> config, ConnectorFactory target)
    {
        return null;
    }
}
