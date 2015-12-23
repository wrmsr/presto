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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.hadoop.config.Config;

import java.util.Map;

public class HiveConfig
        implements Config<HiveConfig>
{
    public static class Metastore
    {
        private Map<String, String> properties = ImmutableMap.of();

        @JsonProperty("properties")
        public Map<String, String> getProperties()
        {
            return properties;
        }

        @JsonProperty("properties")
        public void setProperties(Map<String, String> properties)
        {
            this.properties = properties;
        }

        // https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin
        // Hive metastore listener port. (Hive 1.3.0 and later.)
//        private Integer port;
//
//        @JsonProperty("port")
//        public Integer getPort()
//        {
//            return port;
//        }
//
//        @JsonProperty("port")
//        public void setPort(Integer port)
//        {
//            this.port = port;
//        }

        private boolean run;

        @JsonProperty("run")
        public boolean isRun()
        {
            return run;
        }

        @JsonProperty("run")
        public void setRun(boolean run)
        {
            this.run = run;
        }

        @JsonTypeInfo(
                use = JsonTypeInfo.Id.NAME,
                include = JsonTypeInfo.As.WRAPPER_OBJECT
        )
        @JsonSubTypes({
                @JsonSubTypes.Type(value = MySqlDb.class, name = "mysql"),
                @JsonSubTypes.Type(value = LocalDb.class, name = "local"),
        })
        public static abstract class Db
        {
        }

        public static final class MySqlDb
                extends Db
        {
        }

        public static final class LocalDb
                extends Db
        {
            private String file;

            @JsonProperty("file")
            public String getFile()
            {
                return file;
            }

            @JsonProperty("file")
            public void setFile(String file)
            {
                this.file = file;
            }
        }

        private Db db;

        @JsonProperty("db")
        public Db getDb()
        {
            return db;
        }

        @JsonProperty("db")
        public void setDb(Db db)
        {
            this.db = db;
        }
    }

    private Map<String, Metastore> metastores = ImmutableMap.of();

    @JsonProperty("metastores")
    public Map<String, Metastore> getMetastores()
    {
        return metastores;
    }

    @JsonProperty("metastores")
    public void setMetastores(Map<String, Metastore> metastores)
    {
        this.metastores = metastores;
    }
}
