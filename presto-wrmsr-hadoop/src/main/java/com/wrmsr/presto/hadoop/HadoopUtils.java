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
package com.wrmsr.presto.hadoop;

import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.google.common.io.Files;

import javax.xml.namespace.QName;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map;

public class HadoopUtils
{
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

    public static String renderConfig(Iterable<Map.Entry<String, String>> properties)
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

    public static URL writeConfigs(String fileName, Map<String, String> properties)
            throws Throwable
    {
        File cfgDir = Files.createTempDir();
        File dataDir = Files.createTempDir();
        dataDir.deleteOnExit();

        String xml = renderConfig(properties.entrySet());
        try (FileOutputStream fos = new FileOutputStream(new File(cfgDir, fileName));
                BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            bos.write(xml.getBytes());
        }

        return cfgDir.getAbsoluteFile().toURL();

        // Jvm.addClasspathUrl(Thread.currentThread().getContextClassLoader(), );

        /*
        hiveDefaultURL = arr$.getResource("hive-default.xml");
        hiveSiteURL = arr$.getResource("hive-site.xml");
        hivemetastoreSiteUrl = arr$.getResource("hivemetastore-site.xml");
        hiveServer2SiteUrl = arr$.getResource("hiveserver2-site.xml");
        */
    }
}
