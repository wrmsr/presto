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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.sun.management.OperatingSystemMXBean;
import com.wrmsr.presto.launcher.LauncherFailureException;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import jnr.posix.POSIX;

import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JvmManager
{
    private static final Logger log = Logger.get(JvmManager.class);

    private final JvmConfig jvmConfig;
    private final Set<JvmOptionProvider> jvmOptionProviders;
    private final POSIX posix;

    @GuardedBy("this")
    private File jvm;

    @Inject
    public JvmManager(JvmConfig jvmConfig, Set<JvmOptionProvider> jvmOptionProviders, POSIX posix)
    {
        this.jvmConfig = requireNonNull(jvmConfig);
        this.jvmOptionProviders = ImmutableSet.copyOf(jvmOptionProviders);
        this.posix = requireNonNull(posix);
    }

    @PostConstruct
    @VisibleForTesting
    public synchronized void setupJvm()
    {
        checkState(this.jvm == null);
        File jvm = Paths.get(
                System.getProperty("java.home"),
                "bin",
                "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : "")
        ).toFile();
        checkState(jvm.exists(), "cannot find jvm: " + jvm.getAbsolutePath());
        checkState(jvm.isFile(), "jvm is not a file: " + jvm.getAbsolutePath());
        checkState(jvm.canExecute(), "jvm is not executable: " + jvm.getAbsolutePath());
        this.jvm = jvm;
    }

    public File getJvm()
    {
        return jvm;
    }

    public List<String> getConfigJvmOptions()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JvmOptionProvider p : jvmOptionProviders) {
            builder.addAll(p.getServerJvmArguments());
        }
        builder.addAll(jvmConfig.getOptions());
        return builder.build();
    }

    public List<String> getOverridenJvmOptions()
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMxBean.getInputArguments();
    }

    public List<String> getJvmOptions()
    {
        return ImmutableList.<String>builder()
                .addAll(getConfigJvmOptions())
                .addAll(getOverridenJvmOptions())
                .build();
    }

    public void exec(List<String> args)
    {
        posix.libc().execv(jvm.getAbsolutePath(), args.toArray(new String[args.size()]));
        log.error("JVM Exec failed");
        System.exit(1);
    }
}
