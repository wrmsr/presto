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

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.util.Objects;

@Immutable
public final class FileEntry
        extends Entry
{
    private final File file;

    public FileEntry(String name, File file)
    {
        super(name, file.lastModified());
        this.file = file;
    }

    public File getFile()
    {
        return file;
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
        if (!super.equals(o)) {
            return false;
        }
        FileEntry fileEntry = (FileEntry) o;
        return Objects.equals(file, fileEntry.file);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), file);
    }

    @Override
    public <C, R> R accept(EntryVisitor<C, R> visitor, C context)
    {
        return visitor.visitFileEntry(this, context);
    }
}
