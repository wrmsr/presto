package com.wrmsr.presto.cluster;

public interface Cluster
{
    void bootstrap();

    void start();

    void stop();
}
