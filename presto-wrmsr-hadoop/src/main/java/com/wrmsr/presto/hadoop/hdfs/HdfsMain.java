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
package com.wrmsr.presto.hadoop.hdfs;

/*

balancer org.apache.hadoop.hdfs.server.balancer.Balancer
cacheadmin org.apache.hadoop.hdfs.tools.CacheAdmin
crypto org.apache.hadoop.hdfs.tools.CryptoAdmin
datanode 'org.apache.hadoop.hdfs.server.datanode.DataNode'
dfs org.apache.hadoop.fs.FsShell
dfsadmin org.apache.hadoop.hdfs.tools.DFSAdmin
fetchdt org.apache.hadoop.hdfs.tools.DelegationTokenFetcher
fsck org.apache.hadoop.hdfs.tools.DFSck
getconf org.apache.hadoop.hdfs.tools.GetConf
groups org.apache.hadoop.hdfs.tools.GetGroups
haadmin org.apache.hadoop.hdfs.tools.DFSHAAdmin
jmxget org.apache.hadoop.hdfs.tools.JMXGet
journalnode 'org.apache.hadoop.hdfs.qjournal.server.JournalNode'
lsSnapshottableDir org.apache.hadoop.hdfs.tools.snapshot.LsSnapshottableDir
mover org.apache.hadoop.hdfs.server.mover.Mover
namenode 'org.apache.hadoop.hdfs.server.namenode.NameNode'
nfs3 org.apache.hadoop.hdfs.nfs.nfs3.Nfs3
oev org.apache.hadoop.hdfs.tools.offlineEditsViewer.OfflineEditsViewer
oiv org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewerPB
oiv_legacy org.apache.hadoop.hdfs.tools.offlineImageViewer.OfflineImageViewer
portmap org.apache.hadoop.portmap.Portmap
secondarynamenode 'org.apache.hadoop.hdfs.server.namenode.SecondaryNameNode'
snapshotDiff org.apache.hadoop.hdfs.tools.snapshot.SnapshotDiff
storagepolicies org.apache.hadoop.hdfs.tools.GetStoragePolicies
version org.apache.hadoop.util.VersionInfo
zkfc 'org.apache.hadoop.hdfs.tools.DFSZKFailoverController'

*/

import com.google.common.base.Throwables;
import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Command;
import io.airlift.command.Help;
import io.airlift.log.Logging;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.File;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

public class HdfsMain
{
    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        NameNodeCommand.class,
                        DataNodeCommand.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class PassthroughCommand implements Runnable
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            try {
                runNothrow();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }

        public String[] getArgs()
        {
            return args.toArray(new String[args.size()]);
        }

        public abstract void runNothrow()
                throws Throwable;
    }

    public static abstract class HdfsCommand extends PassthroughCommand
    {
        public static final String NAME_NODE_HOST = "localhost:";
        public static final String WILDCARD_HTTP_HOST = "0.0.0.0:";

        public HdfsConfiguration getConfig() throws Throwable
        {
            HdfsConfiguration config = new HdfsConfiguration(); File hdfsDir = new File(System.getProperty("user.home") + "/hdfs");

            config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(new File(hdfsDir, "name")).toString());
            config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, new File(hdfsDir, "data").getPath());
            config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
            config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
            config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
            config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(new File(hdfsDir, "secondary")).toString());
            config.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, WILDCARD_HTTP_HOST + "0");

            return config;
        }
    }

    /*
    public static abstract class SimpleHdfsCommand extends HdfsCommand
    {
        private final String fqcn;

        protected SimpleHdfsCommand(String fqcn)
        {
            this.fqcn = fqcn;
        }

        @Override
        public void runNothrow()
                throws Throwable
        {
            Logging.initialize();

            // ExitUtil.disableSystemExit();
            // ExitUtil.resetFirstExitException();

            Class<?> cls = Class.forName(fqcn);
            cls.getDeclaredMethod("main", String[].class).invoke(null, new Object[]{getArgs()});
        }
    }
    */

    @Command(name = "namenode", description = "Starts HDFS NameNode")
    public static class NameNodeCommand extends HdfsCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            Logging.initialize();

            // ExitUtil.disableSystemExit();
            // ExitUtil.resetFirstExitException();

            HdfsConfiguration config = getConfig();
            FileSystem.setDefaultUri(config, "hdfs://" + NAME_NODE_HOST + "0");
            NameNode nn = NameNode.createNameNode(getArgs(), config);
        }
    }

    @Command(name = "datanode", description = "Starts HDFS DataNode")
    public static class DataNodeCommand extends HdfsCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            Logging.initialize();

            // ExitUtil.disableSystemExit();
            // ExitUtil.resetFirstExitException();

            HdfsConfiguration config = getConfig();
            FileSystem.setDefaultUri(config, "hdfs://" + NAME_NODE_HOST + "0");
            DataNode dn = DataNode.createDataNode(getArgs(), config, null);
        }
    }
}
