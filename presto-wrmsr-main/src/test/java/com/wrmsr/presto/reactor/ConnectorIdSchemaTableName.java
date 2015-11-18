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
package com.wrmsr.presto.reactor;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.IOException;
import java.util.Objects;

public final class ConnectorIdSchemaTableName
{
    @JsonCreator
    public static ConnectorIdSchemaTableName valueOf(String str)
    {
        int pos = str.indexOf('.');
        if (pos < 0) {
            throw new IllegalArgumentException("Invalid schemaTableName " + str);
        }
        return new ConnectorIdSchemaTableName(str.substring(0, pos), SchemaTableName.valueOf(str.substring(pos + 1)));
    }

    private final String connectorId;
    private final SchemaTableName schemaTableName;

    public ConnectorIdSchemaTableName(String connectorId, SchemaTableName schemaTableName)
    {
        this.connectorId = connectorId;
        this.schemaTableName = schemaTableName;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonValue
    @Override
    public String toString()
    {
        return connectorId + '.' + schemaTableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectorIdSchemaTableName that = (ConnectorIdSchemaTableName) o;
        return Objects.equals(connectorId, that.connectorId) &&
                Objects.equals(schemaTableName, that.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }
}
