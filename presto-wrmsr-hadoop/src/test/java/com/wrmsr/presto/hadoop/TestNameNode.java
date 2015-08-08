package com.wrmsr.presto.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.ExitUtil;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

public class TestNameNode
{
    public static final String NAME_NODE_HOST = "localhost:";
    public static final String WILDCARD_HTTP_HOST = "0.0.0.0:";

    @Test
    public void test() throws Throwable
    {
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
