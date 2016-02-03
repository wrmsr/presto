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
package com.wrmsr.presto.launcher.jvm;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;

import java.io.File;

import static com.google.common.base.Preconditions.checkState;

public class JvmModule
    extends LauncherModule
{
    @Override
    public void configureLauncher(ConfigContainer config, Binder binder)
    {
        binder.bind(Jvm.class).toProvider(JvmModule::getJvm).in(Scopes.SINGLETON);

        binder.bind(JvmManager.class).in(Scopes.SINGLETON);
    }

    private static Jvm getJvm()
    {
        File jvm = new File(System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : ""));
        checkState(jvm.exists() && jvm.isFile());
        return new Jvm(jvm);
    }

}
