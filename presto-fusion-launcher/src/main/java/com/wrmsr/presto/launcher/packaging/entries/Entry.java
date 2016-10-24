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
package com.wrmsr.presto.launcher.packaging.entries;

import java.util.Objects;
import java.util.jar.JarEntry;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class Entry
{
    private final String name;
    private final long time;

    public Entry(String name, long time)
    {
        requireNonNull(name);
        checkArgument(!name.startsWith("/"));
        this.name = name;
        this.time = time;
    }

    public void bestowJarEntryAttributes(JarEntry je)
    {
        je.setTime(time);
    }

    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Entry entry = (Entry) o;
        return time == entry.time &&
                Objects.equals(name, entry.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, time);
    }

    public abstract <C, R> R accept(EntryVisitor<C, R> visitor, C context);
}
