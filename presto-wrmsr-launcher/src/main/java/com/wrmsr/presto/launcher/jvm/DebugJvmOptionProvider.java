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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.wrmsr.presto.launcher.util.JvmConfiguration;

import java.util.List;

public class DebugJvmOptionProvider
        implements JvmOptionProvider
{
    private final JvmConfig jvmConfig;

    @Inject
    public DebugJvmOptionProvider(JvmConfig jvmConfig)
    {
        this.jvmConfig = jvmConfig;
    }

    @Override
    public List<String> getServerJvmArguments()
    {
        JvmConfig.DebugConfig debugConfig = jvmConfig.getDebug();
        if (debugConfig != null && debugConfig.getPort() != null) {
            return ImmutableList.of(
                    JvmConfiguration.DEBUG.valueOf().toString(),
                    JvmConfiguration.REMOTE_DEBUG.valueOf(debugConfig.getPort(), debugConfig.isSuspend()).toString());
        }
        else {
            return ImmutableList.of();
        }
    }
}
