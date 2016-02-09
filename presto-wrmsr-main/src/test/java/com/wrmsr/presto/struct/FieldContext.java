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

public final class FieldContext
{
    private final Struct struct;
    private final int position;

    public FieldContext(int position, Struct struct)
    {
        this.position = position;
        this.struct = struct;
    }

    public Struct getStruct()
    {
        return struct;
    }

    public int getPosition()
    {
        return position;
    }
}
