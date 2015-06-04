package com.facebook.presto.server;

public interface ServerStartupListener
{
    default void onPluginsLoaded()
    {
    }

    default void onConnectorsLoaded()
    {
    }
}
