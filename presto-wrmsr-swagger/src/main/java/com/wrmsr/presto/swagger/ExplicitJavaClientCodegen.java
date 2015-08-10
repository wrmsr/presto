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
package com.wrmsr.presto.swagger;

import io.swagger.codegen.CliOption;
import io.swagger.codegen.CodegenConfig;
import io.swagger.codegen.CodegenType;
import io.swagger.codegen.SupportingFile;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

public class ExplicitJavaClientCodegen extends DefaultCodegen implements CodegenConfig {
    protected String invokerPackage = "io.swagger.client";
    protected String groupId = "io.swagger";
    protected String artifactId = "swagger-java-client";
    protected String artifactVersion = "1.0.0";
    protected String sourceFolder = "src/main/java";

    public ExplicitJavaClientCodegen() {
        super();
        outputFolder = "generated-code/java";
        modelTemplateFiles.put("model.mustache", ".java");
        apiTemplateFiles.put("api.mustache", ".java");
        templateDir = "ExplicitJava";
        apiPackage = "io.swagger.client.api";
        modelPackage = "io.swagger.client.model";

        reservedWords = new HashSet<String>(
                Arrays.asList(
                        "abstract", "continue", "for", "new", "switch", "assert",
                        "default", "if", "package", "synchronized", "boolean", "do", "goto", "private",
                        "this", "break", "double", "implements", "protected", "throw", "byte", "else",
                        "import", "public", "throws", "case", "enum", "instanceof", "return", "transient",
                        "catch", "extends", "int", "short", "try", "char", "final", "interface", "static",
                        "void", "class", "finally", "long", "strictfp", "volatile", "const", "float",
                        "native", "super", "while")
        );

        languageSpecificPrimitives = new HashSet<String>(
                Arrays.asList(
                        "java.lang.String",
                        "boolean",
                        "java.lang.Boolean",
                        "java.lang.Double",
                        "java.lang.Integer",
                        "java.lang.Long",
                        "java.lang.Float",
                        "java.lang.Object")
        );
        instantiationTypes.put("array", "java.util.ArrayList");
        instantiationTypes.put("map", "java.util.HashMap");

        cliOptions.add(new CliOption("invokerPackage", "root package for generated code"));
        cliOptions.add(new CliOption("groupId", "groupId in generated pom.xml"));
        cliOptions.add(new CliOption("artifactId", "artifactId in generated pom.xml"));
        cliOptions.add(new CliOption("artifactVersion", "artifact version in generated pom.xml"));
        cliOptions.add(new CliOption("sourceFolder", "source folder for generated code"));

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

    public CodegenType getTag() {
        return CodegenType.CLIENT;
    }

    public String getName() {
        return "java";
    }

    public String getHelp() {
        return "Generates a Java client library.";
    }

    @Override
    public void processOpts() {
        super.processOpts();

        if (additionalProperties.containsKey("invokerPackage")) {
            this.setInvokerPackage((String) additionalProperties.get("invokerPackage"));
        } else {
            //not set, use default to be passed to template
            additionalProperties.put("invokerPackage", invokerPackage);
        }

        if (additionalProperties.containsKey("groupId")) {
            this.setGroupId((String) additionalProperties.get("groupId"));
        } else {
            //not set, use to be passed to template
            additionalProperties.put("groupId", groupId);
        }

        if (additionalProperties.containsKey("artifactId")) {
            this.setArtifactId((String) additionalProperties.get("artifactId"));
        } else {
            //not set, use to be passed to template
            additionalProperties.put("artifactId", artifactId);
        }

        if (additionalProperties.containsKey("artifactVersion")) {
            this.setArtifactVersion((String) additionalProperties.get("artifactVersion"));
        } else {
            //not set, use to be passed to template
            additionalProperties.put("artifactVersion", artifactVersion);
        }

        if (additionalProperties.containsKey("sourceFolder")) {
            this.setSourceFolder((String) additionalProperties.get("sourceFolder"));
        }

        final String invokerFolder = (sourceFolder + File.separator + invokerPackage).replace(".", File.separator);
        supportingFiles.add(new SupportingFile("pom.mustache", "", "pom.xml"));
        supportingFiles.add(new SupportingFile("ApiClient.mustache", invokerFolder, "ApiClient.java"));
        supportingFiles.add(new SupportingFile("apiException.mustache", invokerFolder, "ApiException.java"));
        supportingFiles.add(new SupportingFile("Configuration.mustache", invokerFolder, "Configuration.java"));
        supportingFiles.add(new SupportingFile("JsonUtil.mustache", invokerFolder, "JsonUtil.java"));
        supportingFiles.add(new SupportingFile("StringUtil.mustache", invokerFolder, "StringUtil.java"));

        final String authFolder = (sourceFolder + File.separator + invokerPackage + ".auth").replace(".", File.separator);
        supportingFiles.add(new SupportingFile("auth/Authentication.mustache", authFolder, "Authentication.java"));
        supportingFiles.add(new SupportingFile("auth/HttpBasicAuth.mustache", authFolder, "HttpBasicAuth.java"));
        supportingFiles.add(new SupportingFile("auth/ApiKeyAuth.mustache", authFolder, "ApiKeyAuth.java"));
        supportingFiles.add(new SupportingFile("auth/OAuth.mustache", authFolder, "OAuth.java"));
    }


    @Override
    public String escapeReservedWord(String name) {
        return "_" + name;
    }

    @Override
    public String apiFileFolder() {
        return outputFolder + "/" + sourceFolder + "/" + apiPackage().replace('.', File.separatorChar);
    }

    public String modelFileFolder() {
        return outputFolder + "/" + sourceFolder + "/" + modelPackage().replace('.', File.separatorChar);
    }

    @Override
    public String toVarName(String name) {
        // replace - with _ e.g. created-at => created_at
        name = name.replaceAll("-", "_");

        // if it's all uppper case, do nothing
        if (name.matches("^[A-Z_]*$")) {
            return name;
        }

        // camelize (lower first character) the variable name
        // pet_id => petId
        name = camelize(name, true);

        // for reserved word or word starting with number, append _
        if (reservedWords.contains(name) || name.matches("^\\d.*")) {
            name = escapeReservedWord(name);
        }

        return name;
    }

    @Override
    public String toParamName(String name) {
        // should be the same as variable name
        return toVarName(name);
    }

    @Override
    public String toModelName(String name) {
        // model name cannot use reserved keyword, e.g. return
        if (reservedWords.contains(name)) {
            throw new RuntimeException(name + " (reserved word) cannot be used as a model name");
        }

        // jfc rewrite this fucking pos
        if (name.startsWith("java.")) {
            return name;
        }

        // camelize the model name
        // phone_number => PhoneNumber
        return camelize(name);
    }

    @Override
    public String toModelFilename(String name) {
        // should be the same as the model name
        return toModelName(name);
    }

    @Override
    public String getTypeDeclaration(Property p) {
        if (p instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty) p;
            Property inner = ap.getItems();
            return getSwaggerType(p) + "<" + getTypeDeclaration(inner) + ">";
        } else if (p instanceof MapProperty) {
            MapProperty mp = (MapProperty) p;
            Property inner = mp.getAdditionalProperties();

            return getSwaggerType(p) + "<String, " + getTypeDeclaration(inner) + ">";
        }
        return super.getTypeDeclaration(p);
    }

    /*
    @Override
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
    */

    @Override
    public String getSwaggerType(Property p) {
        if (p instanceof RefProperty) {
            RefProperty r = (RefProperty) p;
            String datatype = r.get$ref();
            if (datatype.indexOf("#/definitions/") == 0) {
                datatype = datatype.substring("#/definitions/".length());
            }
            return modelPackage + "." + datatype;
        }
        String swaggerType = super.getSwaggerType(p);
        String type = null;
        if (typeMapping.containsKey(swaggerType)) {
            type = typeMapping.get(swaggerType);
            if (languageSpecificPrimitives.contains(type)) {
                return toModelName(type);
            }
        } else {
            type = swaggerType;
        }
        return toModelName(type);
    }

    @Override
    public String toOperationId(String operationId) {
        // method name cannot use reserved keyword, e.g. return
        if (reservedWords.contains(operationId)) {
            throw new RuntimeException(operationId + " (reserved word) cannot be used as method name");
        }

        return camelize(operationId, true);
    }

    public void setInvokerPackage(String invokerPackage) {
        this.invokerPackage = invokerPackage;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setArtifactId(String artifactId) {
        this.artifactId = artifactId;
    }

    public void setArtifactVersion(String artifactVersion) {
        this.artifactVersion = artifactVersion;
    }

    public void setSourceFolder(String sourceFolder) {
        this.sourceFolder = sourceFolder;
    }
}

