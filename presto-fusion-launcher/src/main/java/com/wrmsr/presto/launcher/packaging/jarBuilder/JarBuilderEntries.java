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
package com.wrmsr.presto.launcher.packaging.jarBuilder;

import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.BytesJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.DirectoryJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.FileJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.JarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.JarBuilderEntryVisitor;

public final class JarBuilderEntries
{
    private JarBuilderEntries()
    {
    }

    public static JarBuilderEntry renameJarBuilderEntry(JarBuilderEntry entry, String name)
    {
        return entry.accept(new JarBuilderEntryVisitor<Void, JarBuilderEntry>()
        {
            @Override
            public JarBuilderEntry visitEntry(JarBuilderEntry jarBuilderEntry, Void context)
            {
                throw new IllegalStateException();
            }

            @Override
            public JarBuilderEntry visitBytesEntry(BytesJarBuilderEntry entry, Void context)
            {
                return new BytesJarBuilderEntry(
                        name,
                        entry.getTime(),
                        entry.getBytes());
            }

            @Override
            public JarBuilderEntry visitDirectoryEntry(DirectoryJarBuilderEntry entry, Void context)
            {
                return new DirectoryJarBuilderEntry(
                        name,
                        entry.getTime());
            }

            @Override
            public JarBuilderEntry visitFileEntry(FileJarBuilderEntry entry, Void context)
            {
                return new FileJarBuilderEntry(
                        name,
                        entry.getFile());
            }
        }, null);
    }
}
