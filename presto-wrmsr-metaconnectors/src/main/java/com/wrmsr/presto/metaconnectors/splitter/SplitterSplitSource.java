package com.wrmsr.presto.metaconnectors.splitter;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Created by wtimoney on 5/29/15.
 */
public class SplitterSplitSource
    implements ConnectorSplitSource
{
    private final ConnectorSplitSource target;

    public SplitterSplitSource(ConnectorSplitSource target)
    {
        this.target = target;
    }

    @Override
    public String getDataSourceName()
    {
        return target.getDataSourceName();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        return target.getNextBatch(maxSize).thenApply(l -> l.stream().map(this::split).collect(Collectors.toList()));
    }

    private ConnectorSplit split(ConnectorSplit split)
    {
        return new SplitterSplit(split);
    }

    @Override
    public void close()
    {
        target.close();
    }

    @Override
    public boolean isFinished()
    {
        return target.isFinished();
    }
}
