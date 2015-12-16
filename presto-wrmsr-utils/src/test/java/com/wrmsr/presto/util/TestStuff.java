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
package com.wrmsr.presto.util;

import com.wrmsr.presto.util.codec.Codec;
import org.testng.annotations.Test;

public class TestStuff
{
    @Test
    public void testStuff() throws Throwable
    {
        Codec<Integer, Integer> add2 = Codec.of(i -> i + 2, i -> i - 2);
        Codec<Integer, Integer> mul3 = Codec.of(i -> i * 3, i -> i / 3);
        Codec<Integer, Integer> cii = Codec.compose(add2, mul3);
        cii.encode(10);

        Codec<Float, Integer> toInt = Codec.of(f -> (int) (float) f, i -> (float) i);
        Codec<Float, Integer> cfi = Codec.compose(toInt, add2);
        cfi.encode(10.0f);
    }
}
