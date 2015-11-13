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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/*
public final class Event
{
    public enum Operation
    {
        INSERT,
        UPDATE,
        DELETE
    }

    public interface Sequence
    {
    }

    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final Operation operation;
    private final Sequence sequence;
    private final Optional<Tuple> before;
    private final Optional<Tuple> after;

    public Event(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("operation") Operation operation,
            @JsonProperty("sequence") Sequence sequence,
            @JsonProperty("before") Optional<Tuple> before,
            @JsonProperty("after") Optional<Tuple> after)
    {
        this.connectorId = connectorId;
        this.schemaTableName = schemaTableName;
        this.operation = operation;
        this.sequence = sequence;
        this.before = before;
        this.after = after;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public Operation getOperation()
    {
        return operation;
    }

    @JsonProperty
    public Sequence getSequence()
    {
        return sequence;
    }

    @JsonProperty
    public Optional<Tuple> getBefore()
    {
        return before;
    }

    @JsonProperty
    public Optional<Tuple> getAfter()
    {
        return after;
    }

    @Override
    public String toString()
    {
        return "Event{" +
                "connectorId='" + connectorId + '\'' +
                ", schemaTableName=" + schemaTableName +
                ", operation=" + operation +
                ", sequence=" + sequence +
                ", before=" + before +
                ", after=" + after +
                '}';
    }
}
*/
