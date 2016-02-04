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
package com.wrmsr.presto.launcher;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.util.Box;

import java.util.List;

public interface LauncherCommand
        extends Runnable, AutoCloseable
{
    final class Args
            extends Box<List<String>>
    {
        public Args(List<String> value)
        {
            super(ImmutableList.copyOf(value));
        }
    }

    default void configure(LauncherModule module, List<String> args)
            throws Exception
    {
    }

    default void close()
            throws Exception
    {
    }
}


