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
package com.wrmsr.presto.codec;

import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.spi.type.VarbinaryType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class EncodedParametricType
        implements ParametricType
{
    private final TypeCodec typeCodec;

    public EncodedParametricType(TypeCodec typeCodec)
    {
        this.typeCodec = typeCodec;
    }

    @Override
    public String getName()
    {
        return typeCodec.getName();
    }

    @Override
    public EncodedType createType(List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 1);
        TypeParameter fromTypeParameter = parameters.get(0);
        Type fromType = fromTypeParameter.getType();
        TypeCodec.Specialization specialization = typeCodec.specialize(fromType);
        return new EncodedType(typeCodec, specialization, fromType);
    }
}
