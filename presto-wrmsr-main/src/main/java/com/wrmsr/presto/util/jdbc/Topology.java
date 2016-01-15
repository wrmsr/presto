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
package com.wrmsr.presto.util.jdbc;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.RedactedSerializer;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class Topology
{
    private static final Logger log = Logger.get(Topology.class);

    public static interface Root
            extends Iterable<Group>
    {
        Cluster cluster(String name);
    }

    @JsonIgnoreProperties(value = {"groupsByCluster"}, ignoreUnknown = true)
    public static class RootImpl
            implements Root
    {
        @JsonProperty("topology")
        public final ImmutableList<Group> groups = null;

        protected RootImpl()
        {
        }

        @Override
        public Iterator<Group> iterator()
        {
            return groups.iterator();
        }

        private ImmutableMap<String, Cluster> groupsByCluster = null;

        public Cluster cluster(String name)
        {
            if (groupsByCluster == null) {
                HashMap<String, ImmutableList.Builder<Group>> map = new HashMap<>();
                for (Group group : groups) {
                    ImmutableList.Builder<Group> builder;
                    if (map.containsKey(group.cluster)) {
                        builder = map.get(group.cluster);
                    }
                    else {
                        builder = ImmutableList.builder();
                        map.put(group.cluster, builder);
                    }
                    builder.add(group);
                }
                ImmutableMap.Builder<String, Cluster> builder = ImmutableMap.builder();
                for (Map.Entry<String, ImmutableList.Builder<Group>> entry : map.entrySet()) {
                    builder.put(entry.getKey(), new Cluster(entry.getKey(),
                            entry.getValue().build()));
                }
                groupsByCluster = builder.build();
            }
            return groupsByCluster.get(name);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Cluster
            implements Iterable<Group>
    {
        public final String name;
        public final ImmutableList<Group> groups;

        protected Cluster(String name, ImmutableList<Group> groups)
        {
            this.name = name;
            this.groups = groups;
        }

        @Override
        public Iterator<Group> iterator()
        {
            return groups.iterator();
        }

        private ImmutableMap<String, ImmutableList<Group>> groupsByReplica = null;

        public ImmutableList<Group> replicas(String name)
        {
            if (groupsByReplica == null) {
                HashMap<String, ImmutableList.Builder<Group>> map = new HashMap<>();
                for (Group group : groups) {
                    ImmutableList.Builder<Group> builder;
                    if (map.containsKey(group.replica)) {
                        builder = map.get(group.replica);
                    }
                    else {
                        builder = ImmutableList.builder();
                        map.put(group.replica, builder);
                    }
                    builder.add(group);
                }
                ImmutableMap.Builder<String, ImmutableList<Group>> builder = ImmutableMap.builder();
                for (Map.Entry<String, ImmutableList.Builder<Group>> entry : map.entrySet()) {
                    builder.put(entry.getKey(), entry.getValue().build());
                }
                groupsByReplica = builder.build();
            }
            return groupsByReplica.get(name);
        }

        public Group replica(String name)
        {
            ImmutableList<Group> groups = replicas(name);
            if (groups == null || groups.size() < 1) {
                return null;
            }
            if (groups.size() == 1) {
                return groups.get(0);
            }
            int idx = ThreadLocalRandom.current().nextInt(groups.size());
            return groups.get(idx);
        }
    }

    @JsonIgnoreProperties(value = {"entriesByName"}, ignoreUnknown = true)
    public static class Group
            implements Iterable<Entry>
    {
        public final String cluster = null;
        public final String replica = null;
        public final ImmutableList<Entry> entries = null;
        public final String rdrCluster = null;

        protected Group()
        {
        }

        @Override
        public Iterator<Entry> iterator()
        {
            return entries.iterator();
        }

        private ImmutableMap<String, Entry> entriesByHost = null;

        public Entry host(String host)
        {
            if (entriesByHost == null) {
                ImmutableMap.Builder<String, Entry> builder = ImmutableMap.builder();
                for (Entry entry : entries) {
                    builder.put(entry.host, entry);
                }
                entriesByHost = builder.build();
            }
            return entriesByHost.get(host);
        }

        public Entry entry()
        {
            if (entries == null || entries.size() < 1) {
                return null;
            }
            if (entries.size() == 1) {
                return entries.get(0);
            }
            int idx = ThreadLocalRandom.current().nextInt(entries.size());
            return entries.get(idx);
        }

        /*
        public Connection connect() throws ClassNotFoundException, SQLException {
            return entry().connect();
        }
        */
    }

    public static class ConnectionParams
    {
        public final String connStr;
        public final String user;
        @JsonSerialize(using = RedactedSerializer.class)
        public final String password;

        public ConnectionParams(String connStr, String user, String password)
        {
            this.connStr = connStr;
            this.user = user;
            this.password = password;
        }

        public ConnectionParams(String connStr)
        {
            this(connStr, null, null);
        }

        protected ConnectionParams()
        {
            connStr = null;
            user = null;
            password = null;
        }
    }

    public static String formatConnectionStringProperties(Map<String, Object> map)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                if (sb.length() > 0) {
                    sb.append("&");
                }
                sb.append(String.format("%s=%s", entry.getKey(), entry.getValue()));
            }
        }
        if (sb.length() < 1) {
            return "";
        }
        return "?" + sb.toString();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Entry
    {

        public final String charset = null;
        public final String db = null;
        public final String host = null;
        @JsonProperty("passwd")
        public final String password = null;
        public final Integer port = null;
        public final Boolean useUnicode = null;
        public final String user = null;
        public final Float weight = null;
        public final String localInfile = null;

        protected Entry()
        {
        }

        protected String formatConnectionString(boolean full)
        {
            String base = String.format("jdbc:mysql://%s:%d/%s", host, port, db);
            Map<String, Object> props = new HashMap<>();
            props.put("useUnicode", useUnicode);
            props.put("characterEncoding", charset);
            if (full) {
                props.put("user", user);
                props.put("password", password);
            }
            return base + formatConnectionStringProperties(props);
        }

        private String connectionString;

        public String connectionString()
        {
            if (connectionString == null) {
                connectionString = formatConnectionString(false);
            }
            return connectionString;
        }

        private String fullConnectionString;

        public String fullConnectionString()
        {
            if (fullConnectionString == null) {
                fullConnectionString = formatConnectionString(true);
            }
            return fullConnectionString;
        }

        public ConnectionParams params()
        {
            return new ConnectionParams(connectionString(), user, password);
        }

        /*
        public Connection connect() throws ClassNotFoundException, SQLException {
            return Db.connect(params());
        }
        */
    }

    /*
    public static Topology.Root fromYaml(String yaml) throws IOException {
        return Json.yamlMapper().readValue(yaml, Root.class);
    }
    */

    /*
    public static class RefreshingTopology extends Utils.FileAutoRefreshedValue<Topology.Root>
            implements Root {

        public RefreshingTopology(File file) {
            super(file);
        }

        public RefreshingTopology(String path) {
            super(path);
        }

        public RefreshingTopology(File file, long refreshInterval) {
            super(file, refreshInterval);
        }

        public RefreshingTopology(String path, long refreshInterval) {
            super(path, refreshInterval);
        }

        @Override
        public Root refreshValue() {
            try {
                String yaml = Utils.readFile(file.getPath());
                return Topology.fromYaml(yaml);
            } catch (IOException e) {
                logger.error(e);
                return null;
            }
        }

        @Override
        public Cluster cluster(String name) {
            return value().cluster(name);
        }

        @Override
        public Iterator<Group> iterator() {
            return value().iterator();
        }
    }

    public static void main(String[] args) throws Exception {
        RefreshingTopology topology = new RefreshingTopology("/Users/wtimoney/topology.yaml", 5000);
        try (Connection conn = topology.value().cluster("primary").replica("slave").connect()) {
            System.out.println(Db.scalar(conn, "select max(id) from business;"));
        }
    }
    */
}
