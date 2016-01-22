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

import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Repositories;
import jnr.posix.POSIX;

import java.io.File;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

public abstract class AbstractServerCommand
        extends AbstractDaemonCommand
{
    public void launch()
    {
        LaunchCommand launch = new LaunchCommand();
        if (isNullOrEmpty(Repositories.getRepositoryPath())) {
            launch.getClassloaderUrls();
            String wd = new File(new File(System.getProperty("user.dir")), "presto-main").getAbsolutePath();
            File wdf = new File(wd);
            checkState(wdf.exists() && wdf.isDirectory());
            System.setProperty("user.dir", wd);
            POSIX posix = POSIXUtils.getPOSIX();
            checkState(posix.chdir(wd) == 0);
        }
        for (String s : systemProperties) {
            int i = s.indexOf('=');
            System.setProperty(s.substring(0, i), s.substring(i + 1));
        }
        deleteRepositoryOnExit();
        launch.run();
    }
}
