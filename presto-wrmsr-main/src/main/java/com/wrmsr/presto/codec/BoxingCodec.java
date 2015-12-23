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

import com.google.common.base.Throwables;
import com.wrmsr.presto.util.Box;
import com.wrmsr.presto.util.codec.Codec;

import java.lang.reflect.Constructor;

public class BoxingCodec<B extends Box<V>, V>
        implements Codec<V, B>
{
    private final Class<B> boxCls;
    private final Class<V> valueCls;
    private final Constructor<B> boxCtor;

    public BoxingCodec(Class<B> boxCls, Class<V> valueCls)
    {
        this.boxCls = boxCls;
        this.valueCls = valueCls;

        try {
            boxCtor = boxCls.getDeclaredConstructor(valueCls);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public V decode(B data)
    {
        if (data != null) {
            return data.getValue();
        }
        else {
            return null;
        }
    }

    @Override
    public B encode(V data)
    {
        try {
            return boxCtor.newInstance(data);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }
}
