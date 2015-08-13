package com.wrmsr.presto.launcher.cluster;

import java.io.File;
import java.util.List;
import java.util.Map;

public class Config
{
    public static class Cluster
    {
        private final String name;
        private final List<Node> nodes;

        public Cluster(String name, List<Node> nodes)
        {
            this.name = name;
            this.nodes = nodes;
        }
    }

    public static class Node
    {
        private final String host;
        private final String username;
        private final String password;
        private final File identity;
        private final Map<String, Integer> ports;

        public Node(String host, String username, String password, File identity, Map<String, Integer> ports)
        {
            this.host = host;
            this.username = username;
            this.password = password;
            this.identity = identity;
            this.ports = ports;
        }
    }
}
