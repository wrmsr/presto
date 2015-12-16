/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.cluster;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

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
        private final Optional<String> root;

        public Target(String host, int port, String user, Auth auth, Optional<String> root)
        {
            this.host = host;
            this.port = port;
            this.user = user;
            this.auth = auth;
            this.root = root;
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

        public Optional<String> getRoot()
        {
            return root;
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
