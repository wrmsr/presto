package com.wrmsr.presto.launcher.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class SshClient
{
    protected final String host;
    protected final Optional<Integer> port;
    protected final Optional<String> username;
    protected final Optional<String> password;
    protected final Optional<File> identity;
    protected final boolean forwardAgent;

    public SshClient(
            String host,
            Optional<Integer> port,
            Optional<String> username,
            Optional<String> password,
            Optional<File> identity,
            boolean forwardAgent)
    {
        this.host = checkNotNull(host);
        this.port = checkNotNull(port);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.identity = checkNotNull(identity);
        this.forwardAgent = forwardAgent;
    }

    public interface Session extends AutoCloseable
    {
        OutputStream getOutputStream();
        InputStream getInputStream();
        InputStream getErrorStream();
        int waitFor(long timeoutMilliseconds) throws InterruptedException;
    }

    public abstract Session run(String...  commands) throws IOException;

    public abstract void transfer(boolean send, File src, File dst, long timeoutMilliseconds) throws IOException, InterruptedException;
}
