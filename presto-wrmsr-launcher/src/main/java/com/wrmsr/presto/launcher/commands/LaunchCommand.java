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

import com.google.common.base.Throwables;
import io.airlift.airline.Command;
import io.airlift.log.Logger;

@Command(name = "launch", description = "Launches presto server (argless)")
public final class LaunchCommand
        extends AbstractLauncherCommand
{
    private static final Logger log = Logger.get(LaunchCommand.class);

    @Override
    public void run()
    {
        launchLocal();
    }

    @Override
    public void innerRun()
            throws Throwable
    {
        throw new UnsupportedOperationException();
    }
}
