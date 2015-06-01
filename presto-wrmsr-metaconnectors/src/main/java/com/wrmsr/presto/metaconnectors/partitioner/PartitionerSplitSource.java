package com.wrmsr.presto.metaconnectors.partitioner;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// import static java.util.concurrent.CompletableFuture.completedFuture;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class PartitionerSplitSource
        implements ConnectorSplitSource
{
    private final List<ConnectorSplitSource> targets;

    public PartitionerSplitSource(List<ConnectorSplitSource> target)
    {
        checkState(!checkNotNull(target).isEmpty());
        this.targets = target;
    }

    @Override
    public String getDataSourceName()
    {
        return targets.get(0).getDataSourceName();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        return targets.getNextBatch(maxSize).thenApply(l -> l.stream().map(this::split).collect(Collectors.toList()));
    }

    @Override
    public void close()
    {
        for (ConnectorSplitSource target : targets) {
            target.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        for (ConnectorSplitSource target : targets) {
            if (!target.isFinished()) {
                return false;
            }
        }
        return true;
    }
}

