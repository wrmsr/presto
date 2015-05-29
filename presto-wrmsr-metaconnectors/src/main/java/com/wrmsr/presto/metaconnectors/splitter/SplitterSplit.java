package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;

import java.util.List;

public class SplitterSplit
    implements ConnectorSplit
{
    private final ConnectorSplit target;

    public SplitterSplit(ConnectorSplit target)
    {
        this.target = target;
    }

    public ConnectorSplit getTarget()
    {
        return target;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return target.isRemotelyAccessible();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return target.getAddresses();
    }

    @Override
    public Object getInfo()
    {
        return target.getInfo();
    }
}
