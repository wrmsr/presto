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
package com.wrmsr.presto.hdfs;

import com.google.common.base.Throwables;
import io.airlift.command.Arguments;
import io.airlift.command.Cli;
import io.airlift.command.Help;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class HdfsMain
{
    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class PassthroughCommand implements Runnable
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        @Override
        public void run()
        {
            try {

            }
            catch (Exception e) {
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
}
