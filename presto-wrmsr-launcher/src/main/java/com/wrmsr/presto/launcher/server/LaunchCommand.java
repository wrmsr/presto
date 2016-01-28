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
package com.wrmsr.presto.launcher.server;

import com.wrmsr.presto.launcher.passthrough.AbstractPassthroughCommand;
import io.airlift.airline.Command;

@Command(name = "launch", description = "Launches presto server (argless)")
public final class LaunchCommand
        extends AbstractPassthroughCommand
{
    @Override
    public String getClassName()
    {
        return "com.facebook.presto.server.PrestoServer";
    }

    @Override
    public String getModuleName()
    {
        return "presto-main";
    }
}
