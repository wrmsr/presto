package com.wrmsr.presto.cluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class RemoteRunner
{
    public static abstract class Auth
    {
    }

    public static final class PasswordAuth extends Auth
    {
        private final String password;

        public PasswordAuth(String password)
        {
            this.password = password;
        }

        public String getPassword()
        {
            return password;
        }
    }

    public static final class IdentityFileAuth extends Auth
    {
        private final File identityFile;

        public IdentityFileAuth(File identityFile)
        {
            this.identityFile = identityFile;
        }

        public File getIdentityFile()
        {
            return identityFile;
        }
    }

    public static final class Target
    {
        private final String host;
        private final int port;
        private final String user;
        private final Auth auth;

        public Target(String host, int port, String user, Auth auth)
        {
            this.host = host;
            this.port = port;
            this.user = user;
            this.auth = auth;
        }

        public String getHost()
        {
            return host;
        }

        public int getPort()
        {
            return port;
        }

        public String getUser()
        {
            return user;
        }

        public Auth getAuth()
        {
            return auth;
        }
    }

    @FunctionalInterface
    public interface Handler
    {
        void handle(OutputStream stdin, InputStream stdout, InputStream stderr) throws IOException;
    }

    public abstract void syncDirectories(Target target, File local, String remote);

    public abstract int runCommand(Target target, String command, String... args);

    public abstract int runCommand(Handler handler, Target target, String command, String... args);
}
