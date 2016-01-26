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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.LogRecord;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SubprocessHandler
        extends java.util.logging.Handler
{
    private final Process process;
    private final StaticFormatter formatter = new StaticFormatter();
    private final AtomicBoolean reported = new AtomicBoolean();
    private final Writer writer;

    public SubprocessHandler(String[] args)
    {
        ProcessBuilder builder = new ProcessBuilder(args);
        try {
            process = builder.start();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        writer = new OutputStreamWriter(process.getOutputStream(), UTF_8);
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
        if (!isLoggable(record)) {
            return;
        }

        try {
            writer.write(formatter.format(record));
            writer.flush();
        }
        catch (Exception e) {
            // try to report the first error
            if (!reported.getAndSet(true)) {
                PrintWriter error = new PrintWriter(writer);
                error.print("LOGGING FAILED: ");
                e.printStackTrace(error);
                error.flush();
            }
        }
    }

    @Override
    public void flush()
    {
        try {
            writer.flush();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
