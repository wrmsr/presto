package com.wrmsr.presto.cluster;

import java.io.File;
import java.util.List;

public class SimpleCluster
    implements Cluster
{
    public static class Node
    {
        private final RemoteRunner.Target target;
        private final File root;

        public Node(RemoteRunner.Target target, File root)
        {
            this.target = target;
            this.root = root;
        }

        public RemoteRunner.Target getTarget()
        {
            return target;
        }
    }

    private final List<Node> nodes;
    private final RemoteRunner remoteRunner;
    private final File root;

    public SimpleCluster(List<Node> nodes, RemoteRunner remoteRunner, File root)
    {
        this.nodes = nodes;
        this.remoteRunner = remoteRunner;
        this.root = root;
    }

    @Override
    public void bootstrap()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }
}
