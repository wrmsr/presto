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
package com.wrmsr.presto.launcher;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.wrmsr.presto.launcher.config.LauncherConfigContainer;
import com.wrmsr.presto.launcher.util.DaemonProcess;
import com.wrmsr.presto.launcher.util.JvmConfiguration;
import com.wrmsr.presto.launcher.util.POSIXUtils;
import com.wrmsr.presto.launcher.util.ParentLastURLClassLoader;
import com.wrmsr.presto.util.Repositories;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.resolver.ArtifactResolver;
import jnr.posix.POSIX;
import org.sonatype.aether.artifact.Artifact;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

/*
--heap
--debug-port
--jmx-port

--coordinator
--node-id
--random-node-id
--discovery-uri

com.facebook.presto.server.PluginManager=DEBUG
com.facebook.presto=INFO
com.ning.http.client=WARN
com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory=WARN
coordinator=false
coordinator=true
discovery-server.enabled=true
discovery.uri=http://example.net:8080
distributed-joins-enabled=true
exchange.http-client.connect-timeout=1m
exchange.http-client.max-connections-per-server=1000
exchange.http-client.max-connections=1000
exchange.http-client.read-timeout=1m
experimental-syntax-enabled=true
http-server.http.port=8080
node-scheduler.include-coordinator=false
node-scheduler.include-coordinator=true
node-scheduler.multiple-tasks-per-node-enabled=true
node.data-dir=/Users/wtimoney/presto/data/
node.environment=production
node.environment=test
node.id=ffffffff-ffff-ffff-ffff-ffffffffffff
plugin.dir=/dev/null
presto.version=0.105-SNAPSHOT
presto.version=testversion
query.client.timeout=5m
query.max-age=30m
scheduler.http-client.connect-timeout=1m
scheduler.http-client.max-connections-per-server=1000
scheduler.http-client.max-connections=1000
scheduler.http-client.read-timeout=1m
task.max-memory=1GB

s3
ec2
hdfs
*/

public class LauncherMain
{
    private LauncherMain()
    {
    }

    protected static String[] args; // meh.

    public static void main(String[] args)
            throws Throwable
    {
        // NameStore.getInstance().put("www.google.com", "www.microsoft.com");
        // InetAddress i = InetAddress.getAllByName("www.google.com")[0];
        // System.out.println(i);

        LauncherMain.args = args;

        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Run.class,
                        Launch.class,
                        Start.class,
                        Stop.class,
                        Restart.class,
                        Status.class,
                        Kill.class,
                        CliCommand.class,
                        H2.class,
                        Hive.class,
                        Hdfs.class,
                        Jython.class,
                        Nashorn.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    @Command(name = "launch", description = "Launches presto server (argless)")
    public static class Launch
            implements Runnable
    {
        private List<URL> classloaderUrls;

        public synchronized List<URL> getClassloaderUrls()
        {
            if (classloaderUrls == null) {
                classloaderUrls = ImmutableList.copyOf(resolveModuleClassloaderUrls("presto-main"));
            }
            return classloaderUrls;
        }

        @Override
        public void run()
        {
            runStaticMethod(getClassloaderUrls(), "com.facebook.presto.server.PrestoServer", "main", new Class<?>[] {String[].class}, new Object[] {new String[] {}});
        }
    }

    public static abstract class LauncherCommand
            implements Runnable
    {
        @Option(type = OptionType.GLOBAL, name = {"-c", "--config-file"}, description = "Specify config file path")
        public List<String> configFiles = new ArrayList<>();

        public static File DEFAULT_CONFIG_FILE = new File("presto.yaml");

        // FIXME
        private final int debugPort = 0;
        private final boolean debugSuspend = false;

        // TODO jvm log system

        public LauncherConfigContainer loadConfigContainer()
        {
            /*
            List<File> files;
            if (configFiles.isEmpty()) {
                if (!DEFAULT_CONFIG_FILE.exists()) {
                    return new LauncherConfigContainer();
                }
                else {
                    files = ImmutableList.of(DEFAULT_CONFIG_FILE);
                }
            }
            else {
                files = configFiles.stream().map(File::new).collect(toImmutableList());
            }
            LauncherConfigContainer container = new LauncherConfigContainer();
            for (File file : files) {
                container = container
            }
            */
            throw new UnsupportedOperationException();
        }

        @Override
        public void run()
        {
            if (debugPort > 0) {
                // FIXME recycle repo tmpdir - after this can just brute force exec-exec-exec-exec to desired cfg
                if (!JvmConfiguration.REMOTE_DEBUG.getValue().isPresent()) {
                    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
                    List<String> newArgs = newArrayList(runtimeMxBean.getInputArguments());
                    newArgs.add(0, JvmConfiguration.DEBUG.valueOf().toString());
                    newArgs.add(1, JvmConfiguration.REMOTE_DEBUG.valueOf(debugPort, debugSuspend).toString());
                    String jvmLocation = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java" + (System.getProperty("os.name").startsWith("Win") ? ".exe" : "");
                    try {
                        newArgs.add("-jar");
                        newArgs.add(new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getAbsolutePath());
                    }
                    catch (URISyntaxException e) {
                        throw Throwables.propagate(e);
                    }
                    newArgs.addAll(Arrays.asList(LauncherMain.args));
                    POSIX posix = POSIXUtils.getPOSIX();
                    posix.libc().execv(jvmLocation, newArgs.toArray(new String[newArgs.size()]));
                }
            }
            try {
                innerRun();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }

        public abstract void innerRun()
                throws Throwable;

        public synchronized void deleteRepositoryOnExit()
        {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                File r = new File(Repositories.getRepositoryPath());
                checkState(r.exists() && r.isDirectory());
                Runtime.getRuntime().addShutdownHook(new Thread()
                {
                    @Override
                    public void run()
                    {
                        try {
                            Repositories.removeRecursive(r.toPath());
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                });
            }
        }
    }

    public static abstract class DaemonCommand
            extends LauncherCommand
    {
        private DaemonProcess daemonProcess;

        // FIXME
        private final String pidFile = null;

        public synchronized boolean hasPidFile()
        {
            return !Strings.isNullOrEmpty(pidFile);
        }

        public synchronized DaemonProcess getDaemonProcess()
        {
            if (daemonProcess == null) {
                checkArgument(!Strings.isNullOrEmpty(pidFile), "must set pidfile");
                daemonProcess = new DaemonProcess(new File(pidFile));
            }
            return daemonProcess;
        }
    }

    public static abstract class ServerCommand
            extends DaemonCommand
    {
        @Option(name = {"-r", "--reexec"}, description = "Whether or not to reexec with appropriate JVM settings")
        public boolean reexec;

        @Option(name = {"-D"}, description = "Sets system property")
        public List<String> systemProperties = newArrayList();

        public String[] getExecArgs()
        {
            String java = System.getProperties().getProperty("java.home") + File.separator + "bin" + File.separator + "java";
            checkState(new File(java).exists());
            String jar;
            try {
                jar = new File(LauncherMain.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getAbsolutePath();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            checkState(new File(jar).exists());
            List<String> argv = newArrayList();
            // FIXME repo path + deleteOnExit
            argv.add(java);
            for (String s : systemProperties) {
                argv.add("-D" + s);
            }
            if (Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                argv.add("-D" + Repositories.REPOSITORY_PATH_PROPERTY_KEY + "=" + Repositories.getRepositoryPath());
            }
            argv.add("-jar");
            argv.add(jar);
            argv.add("launch");
            return argv.toArray(new String[argv.size()]);
        }

        public void reexec()
        {
            POSIX posix = POSIXUtils.getPOSIX();
            String[] args = getExecArgs();
            posix.libc().execv(args[0], args);
        }

        public void launch()
        {
            Launch launch = reexec ? null : new Launch();
            if (Strings.isNullOrEmpty(System.getProperty("plugin.preloaded"))) {
                System.setProperty("plugin.preloaded", "|presto-wrmsr-main");
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.environment"))) {
                System.setProperty("node.environment", "development");
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.id"))) {
                System.setProperty("node.id", UUID.randomUUID().toString());
            }
            if (Strings.isNullOrEmpty(System.getProperty("presto.version"))) {
                System.setProperty("presto.version", deducePrestoVersion());
            }
            if (Strings.isNullOrEmpty(System.getProperty("node.coordinator"))) {
                System.setProperty("node.coordinator", "true");
            }
            if (Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                if (launch != null) {
                    launch.getClassloaderUrls();
                }
                String wd = new File(new File(System.getProperty("user.dir")), "presto-main").getAbsolutePath();
                File wdf = new File(wd);
                checkState(wdf.exists() && wdf.isDirectory());
                System.setProperty("user.dir", wd);
                POSIX posix = POSIXUtils.getPOSIX();
                checkState(posix.chdir(wd) == 0);
            }
            if (launch == null) {
                reexec();
            }
            else {
                for (String s : systemProperties) {
                    int i = s.indexOf('=');
                    System.setProperty(s.substring(0, i), s.substring(i + 1));
                }
                deleteRepositoryOnExit();
                launch.run();
            }
        }
    }

    @Command(name = "run", description = "Runs presto server")
    public static class Run
            extends ServerCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            if (hasPidFile()) {
                getDaemonProcess().writePid();
            }
            launch();
        }
    }

    public abstract static class StartCommand
            extends ServerCommand
    {
        public void run(boolean restart)
        {
            if (hasPidFile()) {
                getDaemonProcess().writePid();
            }
            // launch(); dameonize
        }
    }

    @Command(name = "start", description = "Starts presto server")
    public static class Start
            extends StartCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            run(false);
        }
    }

    @Command(name = "stop", description = "Stops presto server")
    public static class Stop
            extends DaemonCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            getDaemonProcess().stop();
        }
    }

    @Command(name = "restart", description = "Restarts presto server")
    public static class Restart
            extends StartCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            getDaemonProcess().stop();
            run(true);
        }
    }

    @Command(name = "status", description = "Gets status of presto server")
    public static class Status
            extends DaemonCommand
    {
        @Override
        public void innerRun()
                throws Throwable
        {
            if (!getDaemonProcess().alive()) {
                System.exit(DaemonProcess.LSB_NOT_RUNNING);
            }
            System.out.println(getDaemonProcess().readPid());
        }
    }

    @Command(name = "kill", description = "Kills presto server")
    public static class Kill
            extends DaemonCommand
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void innerRun()
                throws Throwable
        {
            deleteRepositoryOnExit();
            if (args.isEmpty()) {
                getDaemonProcess().kill();
            }
            else if (args.size() == 1) {
                int signal = Integer.valueOf(args.get(0));
                getDaemonProcess().kill(signal);
            }
            else {
                throw new IllegalArgumentException();
            }
        }
    }

    private static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public static String deducePrestoVersion()
    {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        }
        catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
        InputStream pomIn = LauncherMain.class.getClassLoader().getResourceAsStream("META-INF/maven/com.wrmsr.presto/presto-wrmsr-launcher/pom.xml");
        if (pomIn == null) {
            try {
                pomIn = new FileInputStream(new File("presto-wrmsr-launcher/pom.xml"));
            }
            catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Document doc;
        try {
            doc = dBuilder.parse(pomIn);
        }
        catch (SAXException | IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                pomIn.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Node project = doc.getDocumentElement();
        project.normalize();
        NodeList projectChildren = project.getChildNodes();
        Optional<Node> parent = IntStream.range(0, projectChildren.getLength()).boxed()
                .map(projectChildren::item)
                .filter(n -> "parent".equals(n.getNodeName()))
                .findFirst();
        NodeList parentChildren = parent.get().getChildNodes();
        Optional<Node> version = IntStream.range(0, parentChildren.getLength()).boxed()
                .map(parentChildren::item)
                .filter(n -> "version".equals(n.getNodeName()))
                .findFirst();
        return version.get().getTextContent();
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

    public static void runStaticMethod(String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        try {
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Class cls = cl.loadClass(className);
            Method main = cls.getMethod(methodName, parameterTypes);
            main.invoke(null, args);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static void runStaticMethod(List<URL> urls, String className, String methodName, Class<?>[] parameterTypes, Object[] args)
    {
        Thread t = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    ClassLoader cl = new URLClassLoader(urls.toArray(new URL[urls.size()]), getContextClassLoader().getParent());
                    Thread.currentThread().setContextClassLoader(cl);
                    runStaticMethod(className, methodName, parameterTypes, args);
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
        }
    }

    public static abstract class PassthroughCommand
            extends LauncherCommand
    {
        public abstract String getModuleName();

        public abstract String getClassName();

        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void innerRun()
                throws Throwable
        {
            deleteRepositoryOnExit();
            String moduleName = getModuleName();
            Class<?>[] parameterTypes = new Class<?>[] {String[].class};
            Object[] args = new Object[] {this.args.toArray(new String[this.args.size()])};
            if (moduleName == null) {
                runStaticMethod(getClassName(), "main", parameterTypes, args);
            }
            else {
                runStaticMethod(resolveModuleClassloaderUrls(moduleName), getClassName(), "main", parameterTypes, args);
            }
        }
    }

    @Command(name = "cli", description = "Starts presto cli")
    public static class CliCommand
            extends PassthroughCommand
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

    @Command(name = "hive", description = "Executes Hive command")
    public static class Hive
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-hadoop";
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.hadoop.hive.HiveMain";
        }
    }

    @Command(name = "hdfs", description = "Executes HDFS command")
    public static class Hdfs
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-hadoop";
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.hadoop.hdfs.HdfsMain";
        }
    }

    @Command(name = "h2", description = "Execute H2 command")
    public static class H2
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return null;
        }

        @Override
        public String getClassName()
        {
            return "com.wrmsr.presto.launcher.H2Main";
        }
    }

    @Command(name = "jython", description = "Starts Jython shell")
    public static class Jython
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return "presto-wrmsr-jython";
        }

        @Override
        public String getClassName()
        {
            return "org.python.util.jython";
        }
    }

    @Command(name = "nashorn", description = "Starts Nashorn shell")
    public static class Nashorn
            extends PassthroughCommand
    {
        @Override
        public String getModuleName()
        {
            return null;
        }

        @Override
        public String getClassName()
        {
            return "jdk.nashorn.tools.Shell";
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
