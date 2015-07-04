package com.wrmsr.presto.swagger;

import io.swagger.codegen.languages.JavaClientCodegen;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;

public class ExplicitJavaClientCodegen extends JavaClientCodegen
{
    public ExplicitJavaClientCodegen()
    {
        super();
        modelTemplateFiles.put("explicitmodel.mustache", ".java");
        apiTemplateFiles.put("explicitapi.mustache", ".java");

        typeMapping.put("array", "java.util.List");
        typeMapping.put("map", "java.util.Map");
        typeMapping.put("List", "java.util.List");
        typeMapping.put("boolean", "java.lang.Boolean");
        typeMapping.put("string", "java.lang.String");
        typeMapping.put("int", "java.lang.Integer");
        typeMapping.put("float", "java.lang.Float");
        typeMapping.put("number", "java.math.BigDecimal");
        typeMapping.put("DateTime", "java.util.Date");
        typeMapping.put("long", "java.lang.Long");
        typeMapping.put("short", "java.lang.Short");
        typeMapping.put("char", "java.lang.String");
        typeMapping.put("double", "java.lang.Double");
        typeMapping.put("object", "java.lang.Object");
        typeMapping.put("integer", "java.lang.Integer");

        importMapping.clear();
        /*
        importMapping.put("UUID", "java.util.UUID");
        importMapping.put("File", "java.io.File");
        importMapping.put("Date", "java.util.Date");
        importMapping.put("Timestamp", "java.sql.Timestamp");
        importMapping.put("Map", "");
        importMapping.put("HashMap", "java.util.HashMap");
        importMapping.put("Array", "java.util.List");
        importMapping.put("ArrayList", "java.util.ArrayList");
        importMapping.put("List", "java.util.List");
        importMapping.put("Set", "java.util.Set");
        importMapping.put("DateTime", "org.joda.time.DateTime");
        importMapping.put("LocalDateTime", "org.joda.time.LocalDateTime");
        importMapping.put("LocalDate", "org.joda.time.LocalDate");
        importMapping.put("LocalTime", "org.joda.time.LocalTime");
        */

        languageSpecificPrimitives.clear();
        instantiationTypes.put("array", "java.util.ArrayList");
        instantiationTypes.put("map", "java.util.HashMap");
    }

    public String getSwaggerType(Property p) {
        if (p instanceof RefProperty) {
            RefProperty r = (RefProperty) p;
            String datatype = r.get$ref();
            if (datatype.indexOf("#/definitions/") == 0) {
                datatype = datatype.substring("#/definitions/".length());
            }
            return modelPackage + "." + datatype;
        } else {
            return super.getSwaggerType(p);
        }
    }
}
