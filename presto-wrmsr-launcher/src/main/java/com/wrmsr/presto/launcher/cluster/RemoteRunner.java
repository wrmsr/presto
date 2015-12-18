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
package com.wrmsr.presto.launcher.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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
        private String host;
        private int port;
        private String user;
        private Auth auth;
        private String root;

        @JsonCreator
        public Target()
        {
        }

        public Target(String host, int port, String user, Auth auth, String root)
        {
            this.host = host;
            this.port = port;
            this.user = user;
            this.auth = auth;
            this.root = root;
        }

        @JsonProperty("host")
        public String getHost()
        {
            return host;
        }

        @JsonProperty("host")
        public void setHost(String host)
        {
            this.host = host;
        }

        @JsonProperty("port")
        public int getPort()
        {
            return port;
        }

        @JsonProperty("port")
        public void setPort(int port)
        {
            this.port = port;
        }

        @JsonProperty("user")
        public String getUser()
        {
            return user;
        }

        @JsonProperty("user")
        public void setUser(String user)
        {
            this.user = user;
        }

        @JsonProperty("auth")
        public Auth getAuth()
        {
            return auth;
        }

        @JsonProperty("auth")
        public void setAuth(Auth auth)
        {
            this.auth = auth;
        }

        @JsonProperty("root")
        public String getRoot()
        {
            return root;
        }

        @JsonProperty("root")
        public void setRoot(String root)
        {
            this.root = root;
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
