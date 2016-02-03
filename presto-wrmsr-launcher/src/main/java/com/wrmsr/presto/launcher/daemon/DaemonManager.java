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
package com.wrmsr.presto.launcher.daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.jvm.JvmManager;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.log.Logger;
import jnr.posix.POSIX;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Jvm.getThisJarFile;
import static com.wrmsr.presto.util.ShellUtils.shellEscape;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DaemonManager
{
    private static final Logger log = Logger.get(DaemonManager.class);

    private final DaemonConfig daemonConfig;
    private final JvmManager jvmManager;
    private final POSIX posix;

    @GuardedBy("this")
    private volatile DaemonProcess daemonProcess;

    @Inject
    public DaemonManager(DaemonConfig daemonConfig, JvmManager jvmManager, POSIX posix)
    {
        this.daemonConfig = requireNonNull(daemonConfig);
        this.jvmManager = requireNonNull(jvmManager);
        this.posix = requireNonNull(posix);
    }

    @PostConstruct
    @VisibleForTesting
    public synchronized void setupDaemonProcess()
    {
        checkState(daemonProcess == null);
        checkArgument(!isNullOrEmpty(daemonConfig.getPidFile()), "must set pidfile");
        daemonProcess = new DaemonProcess(new File(daemonConfig.getPidFile()), daemonConfig.getPidFileFd());
    }

    @PreDestroy
    @VisibleForTesting
    public synchronized void teardownDaemonProcess()
    {
        requireNonNull(daemonProcess).close();
    }

    private void redirctStdio()
    {

        if (!isNullOrEmpty(daemonConfig.getStdoutFile())) {
            shBuilder.add(">>" + shellEscape(daemonConfig.getStdoutFile()));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        if (!isNullOrEmpty(daemonConfig.getStderrFile())) {
            shBuilder.add("2>>" + shellEscape(daemonConfig.getStderrFile()));
        }
        else {
            shBuilder.add(">/dev/null");
        }

    }

    @GuardedBy("this")
    private void launch()
    {
        requireNonNull(daemonProcess);

        List<String> args = originalArgs.getValue();
        String lastArg = args.get(args.size() - 1);
        checkArgument(lastArg.equals("start") || lastArg.equals("restart"));

        File jvm = this.jvm.getValue();
        ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                .add(jvm.getAbsolutePath());
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
        }

        builder.add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "launcher." + LauncherConfig.PID_FILE_FD_KEY + "=" + daemonProcess.pidFile);

        File jar = getThisJarFile(getClass());
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

        if (!isNullOrEmpty(daemonConfig.getStdoutFile())) {
            shBuilder.add(">>" + shellEscape(daemonConfig.getStdoutFile()));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        if (!isNullOrEmpty(daemonConfig.getStderrFile())) {
            shBuilder.add("2>>" + shellEscape(daemonConfig.getStderrFile()));
        }
        else {
            shBuilder.add(">/dev/null");
        }

        // TODO subprocess stderr to scribe for gc + vmflags

        shBuilder.add("&");

        String cmd = Joiner.on(" ").join(shBuilder.build());

        POSIX posix = requireNonNull(this.posix);
        File sh = new File("/bin/sh");
        checkState(sh.exists() && sh.isFile());
        posix.libc().execv(sh.getAbsolutePath(), sh.getAbsolutePath(), "-c", cmd);
        throw new IllegalStateException("Unreachable");
    }

    public synchronized void run()
    {
        daemonProcess.writePid();
        launch();
    }

    public synchronized void kill()
    {
        requireNonNull(daemonProcess).kill();
    }

    public synchronized void kill(int signal)
    {
        requireNonNull(daemonProcess).kill(signal);
    }

    public synchronized void restart()
    {
        if (daemonProcess.alive()) {
            daemonProcess.stop();
        }
        launch();
    }

    public synchronized void start()
    {
        launch();
    }

    public synchronized OptionalInt status()
    {
        if (!daemonProcess.alive()) {
            return OptionalInt.empty();
        }
        else {
            return OptionalInt.of(daemonProcess.readPid());
        }
    }

    public synchronized void stop()
    {
        daemonProcess.stop();
    }
}
