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

import com.wrmsr.presto.launcher.LauncherSupport;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ClusterCommands
{
    private ClusterCommands()
    {
    }

    private static LauncherSupport launcherSupport;

    public static void main(LauncherSupport launcherSupport, String[] args)
    {
        ClusterCommands.launcherSupport = launcherSupport;

        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        Push.class
                );

        Cli<Runnable> cliParser = builder.build();
        cliParser.parse(args).run();
    }

    @Command(name = "push", description = "Pushes local home to cluster")
    public static class Push
        implements Runnable
    {
        @Arguments(description = "cluster to push to")
        private List<String> args;

        @Override
        public void run()
        {
            checkArgument(args.size() == 1);
            launcherSupport.getConfig();
        }
    }
}
