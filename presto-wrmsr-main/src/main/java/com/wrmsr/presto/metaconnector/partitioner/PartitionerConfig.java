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
package com.wrmsr.presto.metaconnector.partitioner;

import io.airlift.configuration.Config;

import javax.annotation.Nullable;

public class PartitionerConfig
{
    private String targetName;

    public String getTargetName()
    {
        return targetName;
    }

    @Config("target-name")
    public void setTargetName(String targetName)
    {
        this.targetName = targetName;
    }

    @Nullable
    private String targetConnectorName;

    public String getTargetConnectorName()
    {
        return targetConnectorName;
    }

    @Config("target-connector-name")
    public void setTargetConnectorName(String targetConnectorName)
    {
        this.targetConnectorName = targetConnectorName;
    }

    public static class Partition
    {
        private final String tableName;
        private final String columnName;
        private final int minId;
        private final int maxId;

        public Partition(String tableName, String columnName, int minId, int maxId)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.minId = minId;
            this.maxId = maxId;
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public int getMinId()
        {
            return minId;
        }

        public int getMaxId()
        {
            return maxId;
        }

        @Override
        public String toString()
        {
            return "Partition{" +
                    "tableName='" + tableName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", minId=" + minId +
                    ", maxId=" + maxId +
                    '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Partition partition = (Partition) o;

            if (minId != partition.minId) {
                return false;
            }
            if (maxId != partition.maxId) {
                return false;
            }
            if (tableName != null ? !tableName.equals(partition.tableName) : partition.tableName != null) {
                return false;
            }
            return !(columnName != null ? !columnName.equals(partition.columnName) : partition.columnName != null);
        }

        @Override
        public int hashCode()
        {
            int result = tableName != null ? tableName.hashCode() : 0;
            result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
            result = 31 * result + minId;
            result = 31 * result + maxId;
            return result;
        }
    }
}
