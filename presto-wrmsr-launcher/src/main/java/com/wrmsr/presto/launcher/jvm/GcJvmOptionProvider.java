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

import java.util.List;

public class GcJvmOptionProvider
        implements JvmOptionProvider
{
    private final JvmConfig jvmConfig;

    @Inject
    public GcJvmOptionProvider(JvmConfig jvmConfig)
    {
        this.jvmConfig = jvmConfig;
    }

    @Override
    public List<String> getServerJvmArguments()
    {
        JvmConfig.GcConfig gcConfig = jvmConfig.getGc();
        if (gcConfig == null) {
            return ImmutableList.of();
        }
        else if (gcConfig instanceof JvmConfig.G1GcConfig) {
            return ImmutableList.of("-XX:+UseG1GC");
        }
        else if (gcConfig instanceof JvmConfig.CmsGcConfig) {
            return ImmutableList.of("-XX:+UseConcMarkSweepGC");
        }
        else {
            throw new IllegalArgumentException(gcConfig.toString());
        }
    }
}
