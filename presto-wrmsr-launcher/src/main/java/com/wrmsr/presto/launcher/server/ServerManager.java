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
import com.wrmsr.presto.launcher.config.LauncherConfig;
import com.wrmsr.presto.launcher.jvm.JvmManager;
import com.wrmsr.presto.launcher.util.Statics;
import com.wrmsr.presto.util.Artifacts;
import com.wrmsr.presto.util.Repositories;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import jnr.posix.POSIX;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.wrmsr.presto.util.Jvm.formatSystemPropertyJvmOption;
import static com.wrmsr.presto.util.Jvm.getThisJarFile;
import static java.util.Objects.requireNonNull;

public class ServerManager
{
    private static final Logger log = Logger.get(ServerManager.class);

    private final LauncherConfig launcherConfig;
    private final Set<ServerPropertyProvider> serverPropertyProviders;
    private final JvmManager jvmManager;
    private final POSIX posix;

    @Inject
    public ServerManager(LauncherConfig launcherConfig, Set<ServerPropertyProvider> serverPropertyProviders, JvmManager jvmManager, POSIX posix)
    {
        this.launcherConfig = requireNonNull(launcherConfig);
        this.serverPropertyProviders = requireNonNull(serverPropertyProviders);
        this.jvmManager = requireNonNull(jvmManager);
        this.posix = requireNonNull(posix);
    }

    private void reexec(List<String> args)
    {
        File jar = getThisJarFile(getClass());
        if (!jar.isFile()) {
            log.warn("Jvm options specified but not running with a jar file, ignoring");
            return;
        }

        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.addAll(jvmManager.getJvmOptions());
        builder.add(formatSystemPropertyJvmOption(LauncherConfig.DO_NOT_REEXEC_KEY, "true"));
        builder.add("-jar", jar.getAbsolutePath());
        builder.addAll(args);

        jvmManager.exec(builder.build());
        log.error("JVM reexec failed");
        System.exit(1);
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

    private void setCwd()
    {
        if (!isNullOrEmpty(Repositories.getRepositoryPath())) {
            return;
        }

        File wd = new File(System.getProperty("user.dir"), "presto-main");
        checkState(wd.exists() && wd.isDirectory());

        String ap = wd.getAbsolutePath();
        System.setProperty("user.dir", ap);
        checkState(posix.chdir(ap) == 0);
    }

    private void setServerProperties()
    {
        if (isNullOrEmpty(System.getProperty("http-server.log.path"))) {
            if (!isNullOrEmpty(launcherConfig.getHttpLogFile())) {
                System.setProperty("http-server.log.path", launcherConfig.getHttpLogFile());
            }
        }

        if (isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
            System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
        }

        for (ServerPropertyProvider p : serverPropertyProviders) {
            for (Map.Entry<String, String> e : p.getSystemProperties().entrySet()) {
                System.setProperty(e.getKey(), e.getValue());
            }
        }
    }

    private void delay()
    {
        long delay = launcherConfig.getDelay();
        if (delay <= 0) {
            return;
        }

        log.info(String.format("Delaying launch for %d ms", delay));

        try {
            Thread.sleep(delay);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void launch(List<String> args)
    {
        if (!launcherConfig.isDoNotReexec()) {
            reexec(args);
        }

        configureLoggers();
        setCwd();
        setServerProperties();
        delay();

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

    public void run(List<String> args)
    {
        launch(args);
    }
}
