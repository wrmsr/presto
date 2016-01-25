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

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.LauncherUtils;
import com.wrmsr.presto.util.Artifacts;
import io.airlift.airline.Command;
import io.airlift.log.Logger;

import java.net.URL;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;

@Command(name = "launch", description = "Launches presto server (argless)")
public final class LaunchCommand
        implements Runnable
{
    private static final Logger log = Logger.get(LaunchCommand.class);

    private volatile List<URL> classloaderUrls;

    public synchronized List<URL> getClassloaderUrls()
    {
        if (classloaderUrls == null) {
            classloaderUrls = ImmutableList.copyOf(Artifacts.resolveModuleClassloaderUrls("presto-main"));
        }
        return classloaderUrls;
    }

    private void autoConfigure()
    {
        if (isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
            System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
        }
    }

    @Override
    public void run()
    {
        autoConfigure();
        try {
            LauncherUtils.runStaticMethod(getClassloaderUrls(), "com.facebook.presto.server.PrestoServer", "main", new Class<?>[] {String[].class}, new Object[] {new String[] {}});
        }
        catch (Throwable e) {
            try {
                log.error(e);
            }
            catch (Throwable e2) {
            }
            System.exit(1);
        }
    }
}
