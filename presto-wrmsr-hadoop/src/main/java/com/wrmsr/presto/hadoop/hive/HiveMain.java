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

import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.wrmsr.presto.util.Jvm;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import org.apache.hadoop.hive.cli.CliDriver;

import javax.xml.namespace.QName;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

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

        @Override
        public void run()
        {
            try {
                writeConfigs();
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

        /**
         * Helper method that tries to remove unnecessary namespace
         * declaration that default JDK XML parser (SJSXP) sees fit
         * to add.
         */
        protected static String removeSjsxpNamespace(String xml)
        {
            final String match = " xmlns=\"\"";
            int ix = xml.indexOf(match);
            if (ix > 0) {
                xml = xml.substring(0, ix) + xml.substring(ix + match.length());
            }
            return xml;
        }

        public String renderConfig(Iterable<Map.Entry<String, String>> properties)
                throws Throwable
        {
            StringWriter sw = new StringWriter();
            XmlFactory f = new XmlFactory();
            ToXmlGenerator jg = f.createGenerator(sw);

            jg.setNextName(new QName("configuration"));
            jg.writeStartObject();

            for (Map.Entry<String, String> e : properties) {
                jg.setNextName(new QName("property"));
                jg.writeStartObject();
                jg.writeFieldName("name");
                jg.writeString(e.getKey());
                jg.writeFieldName("value");
                jg.writeString(e.getValue());
                jg.writeEndObject();
            }

            jg.writeEndObject();
            jg.close();
            String xml = removeSjsxpNamespace(sw.toString());

            return xml;
        }

        public void writeConfigs()
                throws Throwable
        {
            File cfgDir = Files.createTempDir();
            File dataDir = Files.createTempDir();
            dataDir.deleteOnExit();

            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("hive.metastore.warehouse.dir", dataDir.getAbsolutePath())
                    .build();

            String xml = renderConfig(properties.entrySet());
            for (String f : ImmutableList.of("hive-site.xml")) {
                try (FileOutputStream fos = new FileOutputStream(new File(cfgDir, f));
                        BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                    bos.write(xml.getBytes());
                }
            }

            Jvm.addClasspathUrl(Thread.currentThread().getContextClassLoader(), cfgDir.getAbsoluteFile().toURL());

            /*
            hiveDefaultURL = arr$.getResource("hive-default.xml");
            hiveSiteURL = arr$.getResource("hive-site.xml");
            hivemetastoreSiteUrl = arr$.getResource("hivemetastore-site.xml");
            hiveServer2SiteUrl = arr$.getResource("hiveserver2-site.xml");
            */
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
        public void run()
        {
            super.run();
        }

        @Override
        public void runNothrow()
                throws Throwable
        {
            org.apache.hadoop.hive.metastore.HiveMetaStore.main(getArgs());
        }
    }
}
