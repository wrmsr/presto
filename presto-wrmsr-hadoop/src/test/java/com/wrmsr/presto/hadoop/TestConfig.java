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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.util.Serialization;
import org.testng.annotations.Test;

import javax.xml.namespace.QName;

import java.io.StringWriter;
import java.util.Map;

public class TestConfig
{
    @JacksonXmlRootElement(localName = "property")
    @JsonPropertyOrder({"name", "value"})
    public static class Property
    {
        public String name;
        public String value;

        public Property(String name, String value)
        {
            this.name = name;
            this.value = value;
        }
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
            xml = xml.substring(0, ix) + xml.substring(ix+match.length());
        }
        return xml;
    }

    public String renderConfig(Iterable<Map.Entry<String, String>> properties) throws Throwable
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

    @Test
    public void testConfig() throws Throwable
    {
        ObjectMapper om = Serialization.XML_OBJECT_MAPPER.get();
        Object m = new Property("hi", "there");
        System.out.println(om.writeValueAsString(m));

        StringWriter sw = new StringWriter();
        XmlFactory f = new XmlFactory();
        ToXmlGenerator jg = f.createGenerator(sw);

        jg.setNextName(new QName("root"));
        jg.writeStartObject();
        jg.writeFieldName("a");
        jg.writeString("text");
        jg.writeEndObject();
        jg.close();
        String xml = removeSjsxpNamespace(sw.toString());

        System.out.println(xml);

        System.out.println(renderConfig(ImmutableMap.of("a", "b", "c", "d").entrySet()));
    }
}
