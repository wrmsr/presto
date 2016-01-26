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
package com.wrmsr.presto.launcher;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.commands.AbstractLauncherCommand;
import com.wrmsr.presto.launcher.commands.CliCommand;
import com.wrmsr.presto.launcher.commands.ClusterCommand;
import com.wrmsr.presto.launcher.commands.DaemonCommand;
import com.wrmsr.presto.launcher.commands.H2Command;
import com.wrmsr.presto.launcher.commands.HdfsCommand;
import com.wrmsr.presto.launcher.commands.HiveCommand;
import com.wrmsr.presto.launcher.commands.JarSyncCommand;
import com.wrmsr.presto.launcher.commands.JythonCommand;
import com.wrmsr.presto.launcher.commands.KillCommand;
import com.wrmsr.presto.launcher.commands.LaunchCommand;
import com.wrmsr.presto.launcher.commands.LauncherCommand;
import com.wrmsr.presto.launcher.commands.NashornCommand;
import com.wrmsr.presto.launcher.commands.RestartCommand;
import com.wrmsr.presto.launcher.commands.RunCommand;
import com.wrmsr.presto.launcher.commands.StartCommand;
import com.wrmsr.presto.launcher.commands.StatusCommand;
import com.wrmsr.presto.launcher.commands.StopCommand;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.log.Logger;

import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class LauncherMain
{
    private static final Logger log = Logger.get(LauncherMain.class);

    private LauncherMain()
    {
    }

    public static List<String> rewriteArgs(List<String> args)
    {
        List<String> ret = newArrayList();
        int i = 0;
        for (; i < args.size(); ++i) {
            String s = args.get(i);
            if (!s.startsWith("-")) {
                break;
            }
            if (s.length() > 2 && Character.isUpperCase(s.charAt(1)) && s.contains("=")) {
                ret.add(s.substring(0, 2));
                ret.add(s.substring(2));
            }
            else {
                ret.add(s);
            }
        }
        for (; i < args.size(); ++i) {
            ret.add(args.get(i));
        }
        return ret;
    }

    public static final List<Class<? extends LauncherCommand>> LAUNCHER_COMMANDS = ImmutableList.<Class<? extends LauncherCommand>>builder().add(
            RunCommand.class,
            DaemonCommand.class,
            LaunchCommand.class,
            StartCommand.class,
            StopCommand.class,
            RestartCommand.class,
            StatusCommand.class,
            KillCommand.class,
            ClusterCommand.class,
            CliCommand.class,
            H2Command.class,
            HiveCommand.class,
            HdfsCommand.class,
            JythonCommand.class,
            NashornCommand.class,
            JarSyncCommand.class
    ).build();

    @SuppressWarnings({"unchecked"})
    public static void main(String[] args)
            throws Throwable
    {
        List<String> newArgs = rewriteArgs(Arrays.asList(args));
        args = newArgs.toArray(new String[newArgs.size()]);
        AbstractLauncherCommand.ORIGINAL_ARGS.set(args);

        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class)
                .withCommands((List) LAUNCHER_COMMANDS);

        Cli<Runnable> cliParser = builder.build();
        cliParser.parse(args).run();
    }
}
