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
package com.wrmsr.presto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

public class TestAvro
{
    public static class Employee
    {
        public String name;
        public int age;
        public String[] emails;
        public Employee boss;
    }

    @Test
    public void testStuff() throws Throwable
    {
        String SCHEMA_JSON = "{\n"
                +"\"type\": \"record\",\n"
                +"\"name\": \"Employee\",\n"
                +"\"fields\": [\n"
                +" {\"name\": \"name\", \"type\": \"string\"},\n"
                +" {\"name\": \"age\", \"type\": \"int\"},\n"
                +" {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
                +" {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
                +"]}";
        Schema raw = new Schema.Parser().setValidate(true).parse(SCHEMA_JSON);
        AvroSchema schema = new AvroSchema(raw);

        ObjectMapper mapper = new ObjectMapper(new AvroFactory());

        Employee empl;
        byte[] avroData ;

//        avroData = ... ; // or find an InputStream
//        Employee empl = mapper.reader(Employee.class)
//                .with(schema)
//                .readValue(avroData);

        empl = new Employee();
        empl.name = "hi";
        empl.age = 2;
        empl.emails = new String[]{"blah", "boo"};

        avroData = mapper.writer(schema)
                .writeValueAsBytes(empl);
    }

    @Test
    public void testStuff2() throws Throwable
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(Employee.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);
    }

    @Test
    public void testDeep() throws Throwable
    {
        String mySchema = "{\n" +
                "    \"name\": \"person\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"firstname\", \"type\": \"string\"},\n" +
                "        {\"name\": \"lastname\", \"type\": \"string\"},\n" +
                "        {\n" +
                "            \"name\": \"address\",\n" +
                "            \"type\": {\n" +
                "                        \"type\" : \"record\",\n" +
                "                        \"name\" : \"AddressUSRecord\",\n" +
                "                        \"fields\" : [\n" +
                "                            {\"name\": \"streetaddress\", \"type\": \"string\"},\n" +
                "                            {\"name\": \"city\", \"type\": \"string\"}\n" +
                "                        ]\n" +
                "                    }\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        Schema raw = new Schema.Parser().setValidate(true).parse(mySchema);
        AvroSchema schema = new AvroSchema(raw);
    }
}
