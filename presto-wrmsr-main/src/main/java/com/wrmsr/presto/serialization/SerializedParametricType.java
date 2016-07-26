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
package com.wrmsr.presto.serialization;

import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;

public class SerializedParametricType
        implements ParametricType
{
//    private final SerializationManager serializationManager;
//
//    public SerializedParametricType(SerializationManager serializationManager)
//    {
//        this.serializationManager = serializationManager;
//    }
//
//    @Override
//    public String getName()
//    {
//        return SerializedType.NAME;
//    }
//
//    @Override
//    public Type createType(List<TypeParameter> types, List<Object> literals)
//    {
//        checkArgument(types.size() == 1);
//        checkArgument(literals.size() == 1);
//        checkArgument(literals.get(0) instanceof String);
//        String serializerName = (String) literals.get(0);
//        Serializer serializer = serializationManager.getSerializer(serializerName);
//        return new SerializedType(types.get(0), serializer);
//    }

    @Override
    public String getName()
    {
        return null;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        return null;
    }
}
