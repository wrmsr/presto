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
package com.wrmsr.presto;

import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableSet;

public class TestStructShit
{
    public static class StructFieldDefinition
    {
        private final String name;
        private final TypeSignature type;
        private final boolean nullable;

        @JsonCreator
        public StructFieldDefinition(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeSignature type,
                @JsonProperty("nullable") boolean nullable)
        {
            checkNotNull(name);
            checkNotNull(type);
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        @JsonProperty("name")
        public String getName()
        {
            return name;
        }

        @JsonProperty("type")
        public TypeSignature getType()
        {
            return type;
        }

        @JsonProperty("nullable")
        public boolean isNullable()
        {
            return nullable;
        }
    }

    public static class StructDefinition
    {
        private final String name;
        private final List<StructFieldDefinition> fields;

        @JsonCreator
        public StructDefinition(
                @JsonProperty("name") String name,
                @JsonProperty("fields") List<StructFieldDefinition> fields)
        {
            checkNotNull(name);
            checkNotNull(fields);
            this.name = name;
            this.fields = ImmutableList.copyOf(fields);
            checkArgument(this.fields.stream().map(f -> f.getName()).collect(toImmutableSet()).size() == this.fields.size());
        }

        @JsonProperty("name")
        public String getName()
        {
            return name;
        }

        @JsonProperty("fields")
        public List<StructFieldDefinition> getFields()
        {
            return fields;
        }
    }

    public static class StructField
    {
        private final StructFieldDefinition def;
        private final Struct struct;

        public StructField(StructFieldDefinition def, Struct struct)
        {
            this.def = def;
            this.struct = struct;
        }
    }

    public static class Struct
    {
        private final StructDefinition def;
        private final List<StructField> fields;

        public Struct(StructDefinition def)
        {
//            this.def = def;
//            ImmutableList.Builder<StructField> fields = ImmutableList.builder();
//            for (int i = 0; i < def.getFields().size(); ++i) {
//            (StructFieldDefinition fieldDef = def.getFields().get(i);
//            }
            throw new UnsupportedOperationException();
        }
    }

    public static class StructManager
    {
    }

    @Test
    public void testShit() throws Throwable
    {
    }
}
