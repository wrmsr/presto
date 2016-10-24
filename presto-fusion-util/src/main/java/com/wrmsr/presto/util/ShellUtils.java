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
package com.wrmsr.presto.util;

import com.google.common.io.CharStreams;
import com.wrmsr.presto.util.process.FinalizedProcess;
import com.wrmsr.presto.util.process.FinalizedProcessBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

public final class ShellUtils
{
    private ShellUtils()
    {

    }

    public static String shellEscape(String s)
    {
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

    @Nullable
    public static String shellExec(String... argv)
            throws IOException
    {
        FinalizedProcessBuilder processBuilder = new FinalizedProcessBuilder(argv);
        processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        String output;
        try (FinalizedProcess process = processBuilder.start()) {
            process.getOutputStream().close();
            output = CharStreams.toString(new InputStreamReader(process.getInputStream()));
            try {
                process.waitFor(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            checkState(process.exitValue() == 0);
        }
        return output;
    }
}
