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
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class StructInfo
{
    private final RowType rowType;
    private final Class<?> listBoxClass;
    private final StdSerializer serializer;
    private final StdDeserializer deserializer;

    public StructInfo(RowType rowType, Class<?> listBoxClass, StdSerializer serializer, StdDeserializer deserializer)
    {
        this.rowType = rowType;
        this.listBoxClass = listBoxClass;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public String getName()
    {
        return rowType.getTypeSignature().getBase();
    }

    public RowType getRowType()
    {
        return rowType;
    }

    public Class<?> getListBoxClass()
    {
        return listBoxClass;
    }

    public StdSerializer getSerializer()
    {
        return serializer;
    }

    public StdDeserializer getDeserializer()
    {
        return deserializer;
    }
}
