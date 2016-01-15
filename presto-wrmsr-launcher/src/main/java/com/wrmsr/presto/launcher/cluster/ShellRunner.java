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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.leacox.process.FinalizedProcess;
import com.leacox.process.FinalizedProcessBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.wrmsr.presto.util.Shell.shellEscape;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

public abstract class ShellRunner
{
    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
            @JsonSubTypes.Type(value = PasswordAuth.class, name = "password"),
            @JsonSubTypes.Type(value = IdentityFileAuth.class, name = "identity-file"),
    })
    public static abstract class Auth
    {
    }

    public static final class PasswordAuth
            extends Auth
    {
        private String password;

        @JsonCreator
        public PasswordAuth(
                @JsonProperty("password") String password)
        {
            this.password = password;
        }

        @JsonProperty("password")
        public String getPassword()
        {
            return password;
        }
    }

    public static final class IdentityFileAuth
            extends Auth
    {
        private final String identityFile;

        @JsonCreator
        public IdentityFileAuth(
                @JsonProperty("file") String identityFile)
        {
            this.identityFile = identityFile;
        }

        @JsonProperty("file")
        public String getIdentityFile()
        {
            return identityFile;
        }
    }

    public static final class Target
    {
        private String host;
        private Integer port;
        private String user;
        private Auth auth;
        private String root;

        @JsonCreator
        public Target()
        {
        }

        public Target(String host, Integer port, String user, Auth auth, String root)
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
        public Target setHost(String host)
        {
            this.host = host;
            return this;
        }

        @JsonProperty("port")
        public Integer getPort()
        {
            return port;
        }

        @JsonProperty("port")
        public Target setPort(Integer port)
        {
            this.port = port;
            return this;
        }

        @JsonProperty("user")
        public String getUser()
        {
            return user;
        }

        @JsonProperty("user")
        public Target setUser(String user)
        {
            this.user = user;
            return this;
        }

        @JsonProperty("auth")
        public Auth getAuth()
        {
            return auth;
        }

        @JsonProperty("auth")
        public Target setAuth(Auth auth)
        {
            this.auth = auth;
            return this;
        }

        @JsonProperty("root")
        public String getRoot()
        {
            return root;
        }

        @JsonProperty("root")
        public Target setRoot(String root)
        {
            this.root = root;
            return this;
        }
    }

    @FunctionalInterface
    public interface Handler
    {
        void handle(OutputStream stdin, InputStream stdout, InputStream stderr)
                throws IOException;
    }

    public static final class UnsupportedTargetException
            extends RuntimeException
    {
    }

    protected final long timeout;

    public static final long DEFAULT_TIMEOUT = 60 * 10000;

    public ShellRunner(long timeout)
    {
        this.timeout = timeout;
    }

    public ShellRunner()
    {
        this(DEFAULT_TIMEOUT);
    }

    public abstract void syncDirectories(Target target, File local, String remote);

    public int runCommand(Target target, String command, String... args)
    {
        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(buildRunCommandArgs(target, command, args))
                .gobbleInputStream(true)
                .gobbleErrorStream(true);
        try (FinalizedProcess process = pb.start()) {
            return process.waitFor(timeout);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public int runCommand(Handler handler, Target target, String command, String... args)
    {
        FinalizedProcessBuilder pb = new FinalizedProcessBuilder(buildRunCommandArgs(target, command, args));
        try (FinalizedProcess process = pb.start()) {
            handler.handle(process.getOutputStream(), process.getInputStream(), process.getErrorStream());
            return process.waitFor(timeout);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    protected abstract List<String> buildRunCommandArgs(Target target, String command, String... args);

    protected List<String> buildTrailingRunCommandArgs(Target target, String command, String... args)
    {
        if (!Strings.isNullOrEmpty(target.getRoot())) {
            String escapedCommand = Joiner.on(' ').join(Stream.concat(Stream.of(command), Stream.of(args)).map(s -> shellEscape(s)).collect(toImmutableList()));
            return ImmutableList.of(Joiner.on(' ').join("cd", shellEscape(target.getRoot()), "&&", escapedCommand));
        }
        else {
            return ImmutableList.<String>builder()
                    .add(command)
                    .addAll(Arrays.asList(args))
                    .build();
        }
    }
}
