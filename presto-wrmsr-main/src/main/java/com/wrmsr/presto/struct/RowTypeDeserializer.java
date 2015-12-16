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

import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.wrmsr.presto.util.Box;
import io.airlift.slice.Slice;

import java.io.IOException;

public class RowTypeDeserializer
        extends StdDeserializer<Box<Slice>>
{
    private final RowType rowType;

    public RowTypeDeserializer(RowType rowType, Class sliceBoxClass)
    {
        super(sliceBoxClass);
        this.rowType = rowType;
    }

    @Override
    public Box<Slice> deserialize(JsonParser jp, DeserializationContext ctxt)
            throws IOException
    {
        return null;
    }
}
