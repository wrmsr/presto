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
package com.wrmsr.presto.launcher.commands;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

@Command(name = "kill", description = "Kills presto server")
public final class KillCommand
        extends AbstractLauncherCommand
{
    @Arguments(description = "arguments")
    private List<String> args = newArrayList();

    @Override
    public void innerRun()
            throws Throwable
    {
        deleteRepositoryOnExit();
        if (args.isEmpty()) {
            getDaemonProcess().kill();
        }
        else if (args.size() == 1) {
            int signal = Integer.valueOf(args.get(0));
            getDaemonProcess().kill(signal);
        }
        else {
            throw new IllegalArgumentException();
        }
    }
}
