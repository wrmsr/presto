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
package com.wrmsr.presto.hadoop.hive;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.wrmsr.presto.hadoop.HadoopService;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import org.apache.hadoop.hive.cli.CliDriver;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;

public class HiveMain
{
    public static void main(String[] args)
            throws Throwable
    {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("presto")
                .withDefaultCommand(Help.class)
                .withCommands(
                        Help.class,
                        CliCommand.class,
                        Metastore.class
                );

        Cli<Runnable> gitParser = builder.build();

        gitParser.parse(args).run();
    }

    public static abstract class PassthroughCommand
            implements Runnable
    {
        @Arguments(description = "arguments")
        private List<String> args = newArrayList();

        public String[] getArgs()
        {
            return args.toArray(new String[args.size()]);
        }

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

        public abstract void runNothrow()
                throws Throwable;
    }

    @Command(name = "cli", description = "Starts hive cli")
    public static class CliCommand
            extends PassthroughCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            CliDriver.main(getArgs());
        }
    }

    @Command(name = "metastore", description = "Starts hive metastore")
    public static class Metastore
            extends PassthroughCommand
    {
        @Override
        public void runNothrow()
                throws Throwable
        {
            /*
            METASTORE_CONNECTION_DRIVER("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver",

            javax.jdo.option.ConnectionURL
            "jdbc:derby:;databaseName=metastore_db;create=true",

            hiveDefaultURL = arr$.getResource("hive-default.xml");
            hiveSiteURL = arr$.getResource("hive-site.xml");
            hivemetastoreSiteUrl = arr$.getResource("hivemetastore-site.xml");
            hiveServer2SiteUrl = arr$.getResource("hiveserver2-site.xml");
            */

            File dataDir = Files.createTempDir();
            dataDir.deleteOnExit();

            ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
            builder.put("hive.metastore.warehouse.dir", dataDir.getAbsolutePath());

            HadoopService svc = new HiveMetastoreService(builder.build(), getArgs());
            svc.start();
            svc.join();
        }
    }
}
