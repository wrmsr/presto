package com.wrmsr.presto.launcher;

import com.google.common.base.Throwables;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import org.h2.tools.Console;
import org.h2.tools.Server;
import org.h2.tools.Shell;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class H2Main
{
    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        ServerCommand.class,
                        ShellCommand.class,
                        ConsoleCommand.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class PassthroughCommand
            implements Runnable
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            try {
                runNothrow();
            }
            catch (Throwable e) {
                throw Throwables.propagate(e);
            }
        }

        public String[] getArgs()
        {
            return args.toArray(new String[args.size()]);
        }

        public abstract void runNothrow()
                throws Throwable;
    }

    @Command(name = "server", description = "Starts H2 server")
    public static class ServerCommand
            extends PassthroughCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            Server.createTcpServer(getArgs()).runTool(getArgs());
        }
    }

    @Command(name = "shell", description = "Starts H2 shell")
    public static class ShellCommand
            extends PassthroughCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            Shell.main(getArgs());
        }
    }

    @Command(name = "console", description = "Starts H2 console")
    public static class ConsoleCommand
            extends PassthroughCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            Console.main(getArgs());
        }
    }
}
