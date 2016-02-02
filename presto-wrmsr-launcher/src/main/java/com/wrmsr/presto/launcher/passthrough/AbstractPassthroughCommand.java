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
package com.wrmsr.presto.launcher.passthrough;

import com.wrmsr.presto.launcher.util.Statics;
import com.wrmsr.presto.launcher.AbstractLauncherCommand;
import com.wrmsr.presto.util.Artifacts;
import io.airlift.airline.Arguments;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public abstract class AbstractPassthroughCommand
        extends AbstractLauncherCommand
{
    public abstract String getModuleName();

    public abstract String getClassName();

    @Arguments(description = "arguments")
    private List<String> args = newArrayList();

    @Override
    public void run()
    {
        String moduleName = getModuleName();
        Class<?>[] parameterTypes = new Class<?>[] {String[].class};
        Object[] args = new Object[] {this.args.toArray(new String[this.args.size()])};
        if (moduleName == null) {
            Statics.runStaticMethod(getClassName(), "main", parameterTypes, args);
        }
        else {
            Statics.runStaticMethod(Artifacts.resolveModuleClassloaderUrls(moduleName), getClassName(), "main", parameterTypes, args);
        }
    }
}
