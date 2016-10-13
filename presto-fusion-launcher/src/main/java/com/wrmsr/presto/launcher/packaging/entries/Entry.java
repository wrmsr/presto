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

import java.util.jar.JarEntry;

public abstract class Entry
{
    private final String jarPath;
    private final long time;

    public Entry(String jarPath, long time)
    {
        this.jarPath = jarPath;
        this.time = time;
    }

    public void processEntry(JarEntry je)
    {
        je.setTime(time);
    }

    public String getJarPath()
    {
        return jarPath;
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

        return !(jarPath != null ? !jarPath.equals(entry.jarPath) : entry.jarPath != null);
    }

    @Override
    public int hashCode()
    {
        return jarPath != null ? jarPath.hashCode() : 0;
    }
}
