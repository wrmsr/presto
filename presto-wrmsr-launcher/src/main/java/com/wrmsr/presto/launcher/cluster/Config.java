package com.wrmsr.presto.launcher.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.File;
import java.util.List;
import java.util.Map;

public class Config
{
    public static class Cluster
    {
        private final String name;
        private final List<Node> nodes;
        private final Node defaults;

        @JsonCreator
        public Cluster(
                @JsonProperty("name") String name,
                @JsonProperty("nodes") List<Node> nodes,
                @JsonProperty("defaults") Node defaults)
        {
            this.name = name;
            this.nodes = nodes;
            this.defaults = defaults;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public List<Node> getNodes()
        {
            return nodes;
        }

        @JsonProperty
        public Node getDefaults()
        {
            return defaults;
        }
    }

    public static class Node
    {
        private final String host;
        private final String username;
        private final String password;
        private final File identity;
        private final Map<String, Integer> ports;

        @JsonCreator
        public Node(
                @JsonProperty("host") String host,
                @JsonProperty("username") String username,
                @JsonProperty("password") String password,
                @JsonProperty("identity") File identity,
                @JsonProperty("ports") Map<String, Integer> ports)
        {
            this.host = host;
            this.username = username;
            this.password = password;
            this.identity = identity;
            this.ports = ports;
        }

        @JsonProperty
        public String getHost()
        {
            return host;
        }

        @JsonProperty
        public String getUsername()
        {
            return username;
        }

        @JsonProperty
        public String getPassword()
        {
            return password;
        }

        @JsonProperty
        public File getIdentity()
        {
            return identity;
        }

        @JsonProperty
        public Map<String, Integer> getPorts()
        {
            return ports;
        }
    }
}
