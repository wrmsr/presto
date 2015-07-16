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
package com.wrmsr.presto.wrapper;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.wrmsr.presto.util.Repositories;
import com.wrmsr.presto.wrapper.util.POSIXUtils;
import com.wrmsr.presto.wrapper.util.ParentLastURLClassLoader;
import io.airlift.command.*;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.log.Logger;
import jnr.posix.POSIX;
import jnr.posix.util.Platform;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;

// mesos yarn cli jarsync

/*
# sample nodeId to provide consistency across test runs
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
node.environment=test
http-server.http.port=8080

discovery-server.enabled=true
discovery.uri=http://localhost:8080

exchange.http-client.max-connections=1000
exchange.http-client.max-connections-per-server=1000
exchange.http-client.connect-timeout=1m
exchange.http-client.read-timeout=1m

scheduler.http-client.max-connections=1000
scheduler.http-client.max-connections-per-server=1000
scheduler.http-client.connect-timeout=1m
scheduler.http-client.read-timeout=1m

query.client.timeout=5m
query.max-age=30m

plugin.bundles=../presto-wrmsr-extensions/pom.xml

presto.version=testversion
experimental-syntax-enabled=true
distributed-joins-enabled=true

node-scheduler.multiple-tasks-per-node-enabled=true

com.facebook.presto=INFO
com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory=WARN
com.ning.http.client=WARN
com.facebook.presto.server.PluginManager=DEBUG

coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
task.max-memory=1GB
discovery-server.enabled=true
discovery.uri=http://example.net:8080

coordinator=false
http-server.http.port=8080
task.max-memory=1GB
discovery.uri=http://example.net:8080

coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
task.max-memory=1GB
discovery-server.enabled=true
discovery.uri=http://example.net:8080

plugin.dir=/dev/null
plugin.bundles=presto-wrmsr-extensions

        ImmutableMap<String, String> props = ImmutableMap.<String, String>builder()
                .put("node.environment", "production")
                .put("node.id", "ffffffff-ffff-ffff-ffff-ffffffffffff")
                .put("node.data-dir", "/Users/wtimoney/presto/data/")
                .put("presto.version", "0.105-SNAPSHOT")
                .put("coordinator", "true")
                .put("node-scheduler.include-coordinator", "true")
                .put("http-server.http.port", "8080")
                .put("task.max-memory", "1GB")
                .put("discovery-server.enabled", "true")
                .put("discovery.uri", "http://localhost:8080")
                .build();
*/

public class PrestoWrapperMain
{
    private PrestoWrapperMain()
    {
    }

    public static void main(String[] args)
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Run.class,
                        Start.class,
                        Stop.class,
                        Restart.class,
                        Status.class,
                        Kill.class,
                        CliCommand.class,
                        HiveMetastoreCommand.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class WrapperCommand implements Runnable
    {
        @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode")
        public boolean verbose;

        @Option(name = {"-c", "--config"}, description = "Specify config file path")
        public String configFile;
    }

    public static abstract class DaemonCommand extends WrapperCommand
    {
        @Option(name = {"-p", "--pidfile"}, description = "Specify pidfile path")
        public String pidFile;
    }

    @Command(name = "run", description = "Runs presto server")
    public static class Run extends WrapperCommand
    {
        @Override
        public void run()
        {
            runStaticMethod(resolveModuleClassloaderUrls("presto-main"), "com.facebook.presto.server.PrestoServer", "main", new Class<?>[]{String[].class}, new Object[]{new String[]{}});
        }
    }

    @Command(name = "start", description = "Starts presto server")
    public static class Start extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "stop", description = "Stops presto server")
    public static class Stop extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "restart", description = "Restarts presto server")
    public static class Restart extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "status", description = "Gets status of presto server")
    public static class Status extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }

    @Command(name = "kill", description = "Kills presto server")
    public static class Kill extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public static List<URL> resolveModuleClassloaderUrls(String name)
    {
        try {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                return Repositories.resolveUrlsForModule(name);
            }
            else {
                ArtifactResolver resolver = new ArtifactResolver(
                        ArtifactResolver.USER_LOCAL_REPO,
                        ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));
                List<Artifact> artifacts = resolver.resolvePom(new File(name + "/pom.xml"));

                List<URL> urls = newArrayList();
                for (Artifact artifact : sortedArtifacts(artifacts)) {
                    if (artifact.getFile() == null) {
                        throw new RuntimeException("Could not resolve artifact: " + artifact);
                    }
                    File file = artifact.getFile().getCanonicalFile();
                    urls.add(file.toURI().toURL());
                }
                return urls;
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static ClassLoader constructModuleClassloader(String name)
    {
        return new ParentLastURLClassLoader(resolveModuleClassloaderUrls(name));
    }

    public static void runStaticMethod(List<URL> urls, String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        Thread t = new Thread() {
            @Override
            public void run()
            {
                try {
                    ClassLoader cl = new URLClassLoader(urls.toArray(new URL[urls.size()]), getContextClassLoader().getParent());
                    Thread.currentThread().setContextClassLoader(cl);
                    Class cls = cl.loadClass(className);
                    Method main = cls.getMethod(methodName, parameterTypes);
                    main.invoke(null, args);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
        t.start();
        try {
            t.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    public static abstract class PassthroughCommand extends WrapperCommand
    {
        public abstract String getModuleName();

        public abstract String getClassName();

        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            runStaticMethod(resolveModuleClassloaderUrls(getModuleName()), getClassName(), "main", new Class<?>[]{String[].class}, new Object[]{args.toArray(new String[args.size()])});
        }
    }

    @Command(name = "cli", description = "Starts presto cli")
    public static class CliCommand extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-cli";
        }

        @Override
        public String getClassName()
        {
            return "com.facebook.presto.cli.Presto";
        }
    }

    @Command(name = "hive-metastore", description = "Starts hive metastore")
    public static class HiveMetastoreCommand extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-hive-hadoop2";
        }

        @Override
        public String getClassName()
        {
            return "org.apache.hadoop.hive.metastore.HiveMetaStore";
        }
    }

    /*
    @Command(name = "mesos", description = "Performs mesos operations")
    public static class Mesos extends DaemonCommand
    {
        @Override
        public void run()
        {

        }
    }
    */

    /*
        @Arguments(description = "Patterns of files to be added")
        public List<String> patterns;

        @Option(name = "-i", description = "Add modified contents interactively.")
        public boolean interactive;

        @Option(name = "-n", description = "Do not query remote heads")
        public boolean noQuery;

        @Arguments(description = "Remote to show")
        public String remote;

        @Option(name = "-t", description = "Track only a specific branch")
        public String branch;

        @Arguments(description = "Remote repository to add")
        public List<String> remote;
    */
}
