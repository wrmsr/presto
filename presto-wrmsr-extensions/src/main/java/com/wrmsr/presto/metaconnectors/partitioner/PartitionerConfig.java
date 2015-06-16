package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.Range;
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

        public Range getRange()
        {
            return Range.range(minId, true, maxId, false);
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
