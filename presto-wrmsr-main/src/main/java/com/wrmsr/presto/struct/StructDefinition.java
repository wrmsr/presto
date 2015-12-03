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
