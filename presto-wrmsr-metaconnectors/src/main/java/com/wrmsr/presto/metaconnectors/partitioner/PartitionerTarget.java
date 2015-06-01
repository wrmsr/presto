package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.Connector;

import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionerTarget
{
    private final Connector target;

    public PartitionerTarget(Connector target)
    {
        this.target = checkNotNull(target);
    }

    public Connector getTarget()
    {
        return target;
    }

    @Override
    public String toString()
    {
        return "PartitionerConnectorTarget{" +
                "target=" + target +
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

        PartitionerTarget that = (PartitionerTarget) o;

        return !(target != null ? !target.equals(that.target) : that.target != null);

    }

    @Override
    public int hashCode()
    {
        return target != null ? target.hashCode() : 0;
    }
}
