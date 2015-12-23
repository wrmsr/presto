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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.wrmsr.presto.util.Box;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ListRowTypeSerializer
        extends StdSerializer<Box<List>>
{
    private final RowType rowType;

    public ListRowTypeSerializer(RowType rowType, Class listBoxClass)
    {
        super(listBoxClass);
        this.rowType = rowType;
    }

    @Override
    public void serialize(Box<List> value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException
    {
        checkNotNull(value);
        List list = value.getValue();
        if (list == null) {
            jgen.writeNull();
            return;
        }
        List<RowType.RowField> rowFields = rowType.getFields();
        checkArgument(list.size() == rowFields.size());
        jgen.writeStartArray();
        for (int i = 0; i < list.size(); ++i) {
            jgen.writeObject(list.get(i));
        }
        jgen.writeEndArray();
    }
}
