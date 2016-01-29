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
package com.wrmsr.presto.launcher.server.daemon;

import com.wrmsr.presto.launcher.server.AbstractServerCommand;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import io.airlift.airline.Command;

@Command(name = "status", description = "Gets status of presto server")
public final class StatusCommand
        extends AbstractServerCommand
{
    // TODO optional wait time

    @Override
    public void serverRun()
            throws Throwable
    {
        if (!getDaemonProcess().alive()) {
            System.exit(DaemonProcess.LSB_NOT_RUNNING);
        }
        System.out.println(getDaemonProcess().readPid());
    }
}
