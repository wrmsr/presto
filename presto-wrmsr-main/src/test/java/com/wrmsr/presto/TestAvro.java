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

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.struct.RowTypeConstructorCompiler;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;

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
    public void testStuff()
            throws Throwable
    {
        String SCHEMA_JSON = "{\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"Employee\",\n"
                + "\"fields\": [\n"
                + " {\"name\": \"name\", \"type\": \"string\"},\n"
                + " {\"name\": \"age\", \"type\": \"int\"},\n"
                + " {\"name\": \"emails\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
                + " {\"name\": \"boss\", \"type\": [\"Employee\",\"null\"]}\n"
                + "]}";
        Schema raw = new Schema.Parser().setValidate(true).parse(SCHEMA_JSON);
        AvroSchema schema = new AvroSchema(raw);

        ObjectMapper mapper = new ObjectMapper(new AvroFactory());

        Employee empl;
        byte[] avroData;

//        avroData = ... ; // or find an InputStream
//        Employee empl = mapper.reader(Employee.class)
//                .with(schema)
//                .readValue(avroData);

        empl = new Employee();
        empl.name = "hi";
        empl.age = 2;
        empl.emails = new String[] {"blah", "boo"};

        avroData = mapper.writer(schema)
                .writeValueAsBytes(empl);
    }

    @Test
    public void testStuff2()
            throws Throwable
    {
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(Employee.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);
    }

    @Test
    public void testDeep()
            throws Throwable
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

    public static abstract class StructFieldType
    {

    }

    public static final class StructField
    {
        public final String name;
        public final Class cls;

        public StructField(String name, Class cls)
        {
            this.name = name;
            this.cls = cls;
        }
    }

    public interface Struct
    {
    }

    public abstract class StructSupport<T extends Struct>
    {
        protected final Class<T> cls;
        protected final List<Struct.Field> fields;

        protected final Constructor<T> valuesCtor;

        public StructSupport(Class<T> cls, List<Struct.Field> fields)
        {
            this.cls = cls;
            this.fields = ImmutableList.copyOf(fields);

            try {
                valuesCtor = cls.getConstructor(Object[].class);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        public Class<T> structClass()
        {
            return cls;
        }

        public List<Struct.Field> fields()
        {
            return fields;
        }

        public int size()
        {
            return fields.size();
        }

        public abstract void toValues(T struct, Object[] values);

        public Object[] toValues(T struct)
        {
            Object[] values = new Object[size()];
            toValues(struct, values);
            return values;
        }

        public T fromValues(Object[] values)
        {
            try {
                return valuesCtor.newInstance(values);
            }
            catch (ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        public abstract void writeBlock()
    }

    public interface Blockable
    {

    }

    @Test
    public void testPojoStruct()
            throws Throwable
    {
        List<Pair<String, Class>> fields = ImmutableList.<Pair<String, Class>>builder()
                .add(ImmutablePair.of("x", int.class))
                .add(ImmutablePair.of("y", int.class))
                .build();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName("point2"),
                type(Object.class));

        definition.declareAnnotation(JsonPropertyOrder.class)
                .setValue("value", fields.stream().map(Pair::getKey).collect(toImmutableList()));

        Map<String, FieldDefinition> fieldDefinitions = new HashMap<>();
        for (Pair<String, Class> field : fields) {
            FieldDefinition fieldDefinition = definition.declareField(a(PUBLIC, FINAL), field.getKey(), field.getValue());
            fieldDefinition.declareAnnotation(JsonProperty.class).setValue("value", field.getKey());
            fieldDefinitions.put(field.getKey(), fieldDefinition);
        }

        List<Parameter> parameters = new ArrayList<>();
        for (Pair<String, Class> field : fields) {
            parameters.add(arg(field.getKey(), field.getValue()));
        }

        MethodDefinition methodDefinition = definition.declareConstructor(a(PUBLIC), parameters);
        methodDefinition.declareAnnotation(JsonCreator.class);

        for (int i = 0; i < fields.size(); ++i) {
            methodDefinition.declareParameterAnnotation(JsonProperty.class, i).setValue("value", fields.get(i).getKey());
        }

        Scope scope = methodDefinition.getScope();
        CallSiteBinder binder = new CallSiteBinder();
        ByteCodeBlock body = methodDefinition.getBody();

        body
                .getVariable(scope.getThis())
                .invokeConstructor(Object.class);

        for (Pair<String, Class> field : fields) {
            body
                    .getVariable(scope.getThis())
                    .getVariable(scope.getVariable(field.getKey()))
                    .putField(fieldDefinitions.get(field.getKey()));
        }

        body
                .ret();

        Class pointCls = defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(RowTypeConstructorCompiler.class.getClassLoader()));

        Object obj = pointCls.getDeclaredConstructor(int.class, int.class).newInstance(1, 2);
        OBJECT_MAPPER.get().writeValueAsString(obj);
    }
}
