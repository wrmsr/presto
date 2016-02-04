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

    @SuppressWarnings({"unchecked"})
    public static void main(String[] args)
            throws Throwable
    {
        List<String> newArgs = rewriteArgs(Arrays.asList(args));
        args = newArgs.toArray(new String[newArgs.size()]);

        LauncherModule module = new LauncherMainModule();

        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class);
        module.configureCli(builder);

        Cli<Runnable> cliParser = builder.build();
        Runnable cmd = cliParser.parse(args);
        if (cmd instanceof LauncherCommand) {
            ((LauncherCommand) cmd).configure(module, Arrays.asList(args));
        }
        try {
            cmd.run();
        }
        finally {
            if (cmd instanceof AutoCloseable) {
                ((AutoCloseable) cmd).close();
            }
        }
    }
}
