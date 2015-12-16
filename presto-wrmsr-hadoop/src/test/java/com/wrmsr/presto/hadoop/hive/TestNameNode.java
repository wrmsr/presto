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
package com.wrmsr.presto.hadoop.hive;

import io.airlift.log.Logging;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import org.testng.annotations.Test;

import java.io.File;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

public class TestNameNode
{
    public static final String NAME_NODE_HOST = "localhost:";
    public static final String WILDCARD_HTTP_HOST = "0.0.0.0:";

    @Test
    public void test() throws Throwable
    {
        Logging.initialize();

        ExitUtil.disableSystemExit();
        ExitUtil.resetFirstExitException();
        HdfsConfiguration config = new HdfsConfiguration();
        File hdfsDir = new File("/Users/spinlock/hdfs");

        /*
        if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
            throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
        }
        */
        // LOG.info("--hdfsdir is " + hdfsDir.getAbsolutePath());

        config.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, fileAsURI(new File(hdfsDir, "name")).toString());
        config.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, new File(hdfsDir, "data").getPath());
        config.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "0.0.0.0:0");
        config.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
        config.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "0.0.0.0:0");
        config.set(DFSConfigKeys.DFS_NAMENODE_CHECKPOINT_DIR_KEY, fileAsURI(new File(hdfsDir, "secondary")).toString());
        config.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, WILDCARD_HTTP_HOST + "0");

        FileSystem.setDefaultUri(config, "hdfs://" + NAME_NODE_HOST + "0");

        String[] args;

        args = new String[]{};//"-format"};
        NameNode nn =  NameNode.createNameNode(args, config);
        for (int i = 0; i < 300; ++i) {
            System.out.println(nn.getState());
            Thread.sleep(1000);
        }

        /*
        args = new String[]{};//"-format"};
        DataNode dn = DataNode.createDataNode(args, config, null);
        for (int i = 0; i < 300; ++i) {
            // System.out.println(dn.getMetrics());
            Thread.sleep(1000);
        }
        */
    }
}
