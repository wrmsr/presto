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
package com.wrmsr.presto.launcher.server;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sun.management.OperatingSystemMXBean;
import com.wrmsr.presto.launcher.LauncherFailureException;
import com.wrmsr.presto.launcher.jvm.JvmConfig;
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.jvm.JvmManager;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.launcher.util.Statics;
import com.wrmsr.presto.util.Artifacts;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.units.DataSize;
import jnr.posix.POSIX;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Jvm.getThisJarFile;
import static java.util.Objects.requireNonNull;

public class ServerManager
{
    private static final Logger log = Logger.get(ServerManager.class);

    private final LauncherConfig launcherConfig;
    private final JvmManager jvmManager;

    @Inject
    public ServerManager(LauncherConfig launcherConfig, JvmManager jvmManager)
    {
        this.launcherConfig = launcherConfig;
        this.jvmManager = jvmManager;
    }

    private void autoConfigure()
    {
        if (isNullOrEmpty(System.getProperty("http-server.log.path"))) {
            if (!isNullOrEmpty(launcherConfig.getHttpLogFile())) {
                System.setProperty("http-server.log.path", launcherConfig.getHttpLogFile());
            }
        }
    }

    private void configureLoggers()
    {
        Logging logging = Logging.initialize();
        try {
            logging.configure(new LoggingConfiguration());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        System.setProperty("presto.do-not-initialize-logging", "true");
    }

    private void maybeRexec()
    {
        List<String> jvmOptions = getJvmOptions();
        if (jvmOptions.isEmpty()) {
            return;
        }

        File jar = getThisJarFile(getClass());
        if (!jar.isFile()) {
            log.warn("Jvm options specified but not running with a jar file, ignoring");
            return;
        }

        ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
                .addAll(jvmOptions)
                .addAll(runtimeMxBean.getInputArguments())
                .add("-D" + PrestoConfigs.CONFIG_PROPERTIES_PREFIX + "jvm." + JvmConfig.ALREADY_CONFIGURED_KEY + "=true");
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            builder.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
        }
        builder
                .add("-jar")
                .add(jar.getAbsolutePath())
                .addAll(originalArgs.value);

        List<String> newArgs = builder.build();
        j
        requireNonNull(posix).libc().execv(jvm.getAbsolutePath(), newArgs.toArray(new String[newArgs.size()]));
        throw new IllegalStateException("Unreachable");
    }

    public void launch()
    {
        autoConfigure();
        configureLoggers();
        if (isNullOrEmpty(Repositories.getRepositoryPath())) {
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
        long delay = config.getMergedNode(LauncherConfig.class).getDelay();
        if (delay > 0) {
            log.info(String.format("Delaying launch for %d ms", delay));
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        launchLocal();
    }

    public void launchLocal()
    {
        if (isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
            System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
        }

        List<URL> classloaderUrls = ImmutableList.copyOf(Artifacts.resolveModuleClassloaderUrls("presto-main"));

        try {
            Statics.runStaticMethod(classloaderUrls, "com.facebook.presto.server.PrestoServer", "main", new Class<?>[] {String[].class}, new Object[] {new String[] {}});
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

    public void run()
    {
        maybeRexec();
        launch();
    }
}
