package com.wrmsr.presto.launcher.util;

import com.google.common.collect.ImmutableList;
import com.leacox.process.FinalizedProcess;
import com.leacox.process.FinalizedProcessBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class SubprocessSshClient extends SshClient
{
    public SubprocessSshClient(
            String host,
            Optional<Integer> port,
            Optional<String> username,
            Optional<String> password,
            Optional<File> identity,
            boolean forwardAgent)
    {
        super(host, port, username, password, identity, forwardAgent);
    }

    public class Session implements SshClient.Session
    {
        private final FinalizedProcess process;

        private Session(FinalizedProcess process)
        {
            this.process = process;
        }

        @Override
        public OutputStream getOutputStream()
        {
            return process.getOutputStream();
        }

        @Override
        public InputStream getInputStream()
        {
            return process.getInputStream();
        }

        @Override
        public InputStream getErrorStream()
        {
            return process.getErrorStream();
        }

        @Override
        public int waitFor(long timeoutMilliseconds) throws InterruptedException
        {
            return process.waitFor(timeoutMilliseconds);
        }

        @Override
        public void close()
                throws Exception
        {
            process.close();
        }
    }

    protected List<String> getCommonArgs()
    {
        ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
        if (port.isPresent()) {
            argsBuilder.add("-p", port.get().toString());
        }
        if (identity.isPresent()) {
            argsBuilder.add("-i", identity.get().getAbsolutePath());
        }
        if (forwardAgent) {
            argsBuilder.add("-A");
        }
        String usernameAndHost = "";
        if (username.isPresent()) {
            usernameAndHost += username.get() + "@";
        }
        usernameAndHost += host;
        argsBuilder.add(usernameAndHost);
        return argsBuilder.build();
    }

    @Override
    public Session run(String... commands) throws IOException
    {
        ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
        argsBuilder.add("ssh");
        argsBuilder.addAll(getCommonArgs());
        argsBuilder.addAll(Arrays.asList(commands));
        FinalizedProcessBuilder processBuilder = new FinalizedProcessBuilder(argsBuilder.build());
        return new Session(processBuilder.start());
    }

    @Override
    public void transfer(boolean send, File src, File dst, long timeoutMilliseconds) throws IOException, InterruptedException
    {
        ImmutableList.Builder<String> argsBuilder = ImmutableList.builder();
        argsBuilder.add("scp");
        argsBuilder.addAll(getCommonArgs());
        if (send) {
            argsBuilder.add(src.toString(), dst.toString());
        }
        else {
            argsBuilder.add(dst.toString(), src.toString());
        }
        FinalizedProcessBuilder processBuilder = new FinalizedProcessBuilder(argsBuilder.build())
                .gobbleInputStream(true)
                .gobbleErrorStream(true);
        try (FinalizedProcess process = processBuilder.start()) {
            int returnValue = process.waitFor(timeoutMilliseconds);
            if (returnValue != 0) {
                throw new IllegalStateException("return code: " + returnValue);
            }
        }
    }
}
