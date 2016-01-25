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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.LauncherMain;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.config.PrestoConfigs;
import jnr.posix.POSIX;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Jvm.getJarFile;
import static com.wrmsr.presto.util.ShellUtils.shellEscape;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public abstract class AbstractStartCommand
        extends AbstractServerCommand
{
    public void run(boolean restart)
    {
        if (getDaemonProcess().alive()) {
            if (restart) {
                getDaemonProcess().stop();
            }
            else {
                return;
            }
        }

        List<String> args = ImmutableList.copyOf(ORIGINAL_ARGS.get());
        String lastArg = args.get(args.size() - 1);
        checkArgument(lastArg.equals("start") || lastArg.equals("restart"));

        File jvm = getJvm();
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                .add(jvm.getAbsolutePath());
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        // builder.addAll(runtimeMxBean.getInputArguments());
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
        }

        builder.add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "launcher." + LauncherConfig.PID_FILE_FD_KEY + "=" + getDaemonProcess().pidFile);

        LauncherConfig config = getConfig().getMergedNode(LauncherConfig.class);

        if (!isNullOrEmpty(config.getLogFile())) {
            builder.add("-Dlog.output-file=" + replaceVars(config.getLogFile()));
            builder.add("-Dlog.enable-console=false");
        }

        File jar = getJarFile(getClass());
        checkState(jar.isFile());

        builder
                .add("-jar")
                .add(jar.getAbsolutePath())
                .addAll(IntStream.range(0, args.size() - 1).boxed().map(args::get).collect(toImmutableList()))
                .add("daemon");

        ImmutableList.Builder<String> shBuilder = ImmutableList.<String>builder()
                // .add("setsid")
                .addAll(builder.build().stream().map(s -> shellEscape(s)).collect(toImmutableList()));
        shBuilder.add("</dev/null");

        if (!isNullOrEmpty(config.getStdoutFile())) {
            shBuilder.add(">>" + shellEscape(replaceVars(config.getStdoutFile())));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        if (!isNullOrEmpty(config.getStderrFile())) {
            shBuilder.add("2>>" + shellEscape(replaceVars(config.getStderrFile())));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        shBuilder.add("&");

        String cmd = Joiner.on(" ").join(shBuilder.build());

        POSIX posix = POSIXUtils.getPOSIX();
        File sh = new File("/bin/sh");
        checkState(sh.exists() && sh.isFile());
        posix.libc().execv(sh.getAbsolutePath(), sh.getAbsolutePath(), "-c", cmd);
        throw new IllegalStateException("Unreachable");
    }
}
