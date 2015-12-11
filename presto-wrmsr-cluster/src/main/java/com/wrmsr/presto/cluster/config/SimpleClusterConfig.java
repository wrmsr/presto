package com.wrmsr.presto.cluster.config;

public class SimpleClusterConfig
{
    public static class Node
    {
        private final String host;
        private final int port;
        private final String user;

        public Node(String host, int port, String user)
        {
            this.host = host;
            this.port = port;
            this.user = user;
        }
    }

    public static final Node NODE_DEFAULTS = new Node(null, 22, null);
}

