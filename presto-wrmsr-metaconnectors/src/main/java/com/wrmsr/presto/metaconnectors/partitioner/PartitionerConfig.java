package com.wrmsr.presto.metaconnectors.partitioner;

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

    private String splitColumnName;

    public String getSplitColumnName()
    {
        return splitColumnName;
    }

    @Config("split-column-name")
    public void setSplitColumnName(String splitColumnName)
    {
        this.splitColumnName = splitColumnName;
    }

    private int numSplits = 2;

    public int getNumSplits()
    {
        return numSplits;
    }

    @Config("num-splits")
    public void setNumSplits(int numSplits)
    {
        this.numSplits = numSplits;
    }
}
