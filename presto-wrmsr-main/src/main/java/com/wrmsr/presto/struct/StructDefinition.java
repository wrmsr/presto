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
package com.wrmsr.presto.struct;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

// FIXME delete
public class StructDefinition
{
    public static class Field
    {
        @Nullable
        private final String name;
        private final String type;

        @JsonCreator
        public Field(
                @JsonProperty("name") @Nullable String name,
                @JsonProperty("type") String type)
        {
            this.name = name;
            this.type = type;
        }

        @Nullable
        public String getName()
        {
            return name;
        }

        public String getType()
        {
            return type;
        }
    }

    private final String name;
    private final List<Field> fields;

    @JsonCreator
    public StructDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("fields") List<Field> fields)
    {
        this.name = name;
        this.fields = fields;
    }

    public String getName()
    {
        return name;
    }

    public List<Field> getFields()
    {
        return fields;
    }
}
