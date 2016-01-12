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
package com.wrmsr.presto.connector.views;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DirectoryViewStorage
        implements ViewStorage
{
    private final File directory;

    public DirectoryViewStorage(File directory)
    {
        this.directory = directory;
    }

    @Override
    public List<String> getViewNames()
    {
        return ImmutableList.copyOf(directory.list());
    }

    @Override
    public String getView(String name)
    {
        try {
            return Files.readFile(new File(directory, name).getPath());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putView(String name, String content)
    {
        try {
            Files.writeFile(new File(directory, name).getPath(), content);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropView(String name)
    {
        if (!new File(directory, name).delete()) {
            throw new RuntimeException("delete failed");
        }
    }
}
