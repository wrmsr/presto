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
package io.airlift.log;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.logging.LogRecord;

public class SubprocessHandler
        extends java.util.logging.Handler
{
    private final Process process;

    public SubprocessHandler(String[] args)
    {
        ProcessBuilder builder = new ProcessBuilder(args);
        try {
            process = builder.start();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close()
            throws SecurityException
    {
        process.destroy();
    }

    @Override
    public void publish(LogRecord record)
    {

    }

    @Override
    public void flush()
    {

    }
}
