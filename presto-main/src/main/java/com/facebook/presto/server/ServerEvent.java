package com.facebook.presto.server;

public abstract class ServerEvent
{
    public interface Listener
    {
        default void onServerEvent(ServerEvent event)
        {
        }
    }

    public static final class PluginsLoaded extends ServerEvent
    {
    }

    public static final class ConnectorsLoaded extends ServerEvent
    {
    }
}
