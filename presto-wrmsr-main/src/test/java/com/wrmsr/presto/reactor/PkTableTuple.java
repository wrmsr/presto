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
package com.wrmsr.presto.reactor;

import java.util.List;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public class PkTableTuple
        extends TableTuple
{
    public PkTableTuple(PkTableTupleLayout layout, List<Object> values)
    {
        super(layout, values);
    }

    public PkTableTupleLayout getPkLayout()
    {
        return (PkTableTupleLayout) layout;
    }

    public List<Object> getPkValues()
    {
        return getPkLayout().getPkIndices().stream().map(values::get).collect(toImmutableList());
    }

    public List<Object> getNkValues()
    {
        return getPkLayout().getNkIndices().stream().map(values::get).collect(toImmutableList());
    }

    public TableTuple getPk()
    {
        return new TableTuple(getPkLayout().getPk(), getPkValues());
    }

    public TableTuple getNk()
    {
        return new TableTuple(getPkLayout().getNk(), getNkValues());
    }
}
