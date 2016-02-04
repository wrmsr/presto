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
import com.sun.management.OperatingSystemMXBean;
import com.wrmsr.presto.launcher.LauncherFailureException;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import io.airlift.units.DataSize;

import java.lang.management.ManagementFactory;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class HeapJvmOptionProvider
    implements JvmOptionProvider
{
    private final JvmConfig jvmConfig;

    @Inject
    public HeapJvmOptionProvider(JvmConfig jvmConfig)
    {
        this.jvmConfig = jvmConfig;
    }

    @Override
    public List<String> getServerJvmArguments()
    {
        JvmConfig.HeapConfig heapConfig = jvmConfig.getHeap();
        if (heapConfig == null) {
            return ImmutableList.of();
        }

        checkArgument(!isNullOrEmpty(heapConfig.getSize()));
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long base;
        if (heapConfig.isFree()) {
            base = os.getFreePhysicalMemorySize();
        }
        else {
            base = os.getTotalPhysicalMemorySize();
        }

        long sz;
        if (heapConfig.getSize().endsWith("%")) {
            sz = ((base * 100) / Integer.parseInt(heapConfig.getSize())) / 100;
        }
        else {
            boolean negative = heapConfig.getSize().startsWith("-");
            sz = (long) DataSize.valueOf(heapConfig.getSize().substring(negative ? 1 : 0)).getValue(DataSize.Unit.BYTE);
            if (negative) {
                sz = base - sz;
            }
        }

        if (heapConfig.getMax() != null) {
            long max = (long) heapConfig.getMax().getValue(DataSize.Unit.BYTE);
            if (sz > max) {
                sz = max;
            }
        }
        if (heapConfig.getMin() != null) {
            long min = (long) heapConfig.getMin().getValue(DataSize.Unit.BYTE);
            if (sz < min) {
                if (heapConfig.isAttempt()) {
                    sz = min;
                }
                else {
                    throw new LauncherFailureException(String.format("Insufficient memory: got %d, need %d", sz, min));
                }
            }
        }
        checkArgument(sz > 0);
        return ImmutableList.of(JvmConfiguration.MAX_HEAP_SIZE.valueOf(new DataSize(sz, DataSize.Unit.BYTE)).toString());
    }
}
