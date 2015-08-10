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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.swagger.codegen.CliOption;
import io.swagger.codegen.CodegenConfig;
import io.swagger.codegen.CodegenModel;
import io.swagger.codegen.CodegenModelFactory;
import io.swagger.codegen.CodegenModelType;
import io.swagger.codegen.CodegenOperation;
import io.swagger.codegen.CodegenParameter;
import io.swagger.codegen.CodegenProperty;
import io.swagger.codegen.CodegenResponse;
import io.swagger.codegen.CodegenSecurity;
import io.swagger.codegen.CodegenType;
import io.swagger.codegen.SupportingFile;
import io.swagger.codegen.examples.ExampleGenerator;
import io.swagger.models.ArrayModel;
import io.swagger.models.ComposedModel;
import io.swagger.models.Model;
import io.swagger.models.ModelImpl;
import io.swagger.models.Operation;
import io.swagger.models.RefModel;
import io.swagger.models.Response;
import io.swagger.models.Swagger;
import io.swagger.models.auth.ApiKeyAuthDefinition;
import io.swagger.models.auth.BasicAuthDefinition;
import io.swagger.models.auth.In;
import io.swagger.models.auth.SecuritySchemeDefinition;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.CookieParameter;
import io.swagger.models.parameters.FormParameter;
import io.swagger.models.parameters.HeaderParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.parameters.SerializableParameter;
import io.swagger.models.properties.AbstractNumericProperty;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.BooleanProperty;
import io.swagger.models.properties.DateProperty;
import io.swagger.models.properties.DateTimeProperty;
import io.swagger.models.properties.DecimalProperty;
import io.swagger.models.properties.DoubleProperty;
import io.swagger.models.properties.FloatProperty;
import io.swagger.models.properties.IntegerProperty;
import io.swagger.models.properties.LongProperty;
import io.swagger.models.properties.MapProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.PropertyBuilder;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.properties.StringProperty;
import io.swagger.util.Json;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExplicitJavaClientCodegen
        implements CodegenConfig
{

    private static final Logger LOGGER = LoggerFactory.getLogger(ExplicitJavaClientCodegen.class);
    private final String fileSuffix = ".java";
    private final Map<String, String> apiTemplateFiles = new HashMap<>();
    private final Map<String, String> modelTemplateFiles = new HashMap<>();
    private final Map<String, Object> additionalProperties = new HashMap<>();
    private final List<SupportingFile> supportingFiles = new ArrayList<>();
    private final List<CliOption> cliOptions = new ArrayList<>();
    private String outputFolder = "";
    private Set<String> defaultIncludes = new HashSet<>();
    private Map<String, String> typeMapping = new HashMap<>();
    private Map<String, String> instantiationTypes = new HashMap<>();
    private Set<String> reservedWords = new HashSet<>();
    private Set<String> languageSpecificPrimitives = new HashSet<>();
    private Map<String, String> importMapping = new HashMap<>();
    private String modelPackage = "";
    private String apiPackage = "";
    private String templateDir;
    private String invokerPackage = "io.swagger.client";
    private String groupId = "io.swagger";
    private String artifactId = "swagger-java-client";
    private String artifactVersion = "1.0.0";
    private String sourceFolder = "src/main/java";

    public ExplicitJavaClientCodegen()
    {
        defaultIncludes = new HashSet<>(
                Arrays.asList("double",
                        "int",
                        "long",
                        "short",
                        "char",
                        "float",
                        "String",
                        "boolean",
                        "Boolean",
                        "Double",
                        "Void",
                        "Integer",
                        "Long",
                        "Float")
        );

        typeMapping = new HashMap<>();
        typeMapping.put("array", "List");
        typeMapping.put("map", "Map");
        typeMapping.put("List", "List");
        typeMapping.put("boolean", "Boolean");
        typeMapping.put("string", "String");
        typeMapping.put("int", "Integer");
        typeMapping.put("float", "Float");
        typeMapping.put("number", "BigDecimal");
        typeMapping.put("DateTime", "Date");
        typeMapping.put("long", "Long");
        typeMapping.put("short", "Short");
        typeMapping.put("char", "String");
        typeMapping.put("double", "Double");
        typeMapping.put("object", "Object");
        typeMapping.put("integer", "Integer");

        instantiationTypes = new HashMap<>();

        reservedWords = new HashSet<>();

        importMapping = new HashMap<>();
        importMapping.put("BigDecimal", "java.math.BigDecimal");
        importMapping.put("UUID", "java.util.UUID");
        importMapping.put("File", "java.io.File");
        importMapping.put("Date", "java.util.Date");
        importMapping.put("Timestamp", "java.sql.Timestamp");
        importMapping.put("Map", "java.util.Map");
        importMapping.put("HashMap", "java.util.HashMap");
        importMapping.put("Array", "java.util.List");
        importMapping.put("ArrayList", "java.util.ArrayList");
        importMapping.put("List", "java.util.*");
        importMapping.put("Set", "java.util.*");
        importMapping.put("DateTime", "org.joda.time.*");
        importMapping.put("LocalDateTime", "org.joda.time.*");
        importMapping.put("LocalDate", "org.joda.time.*");
        importMapping.put("LocalTime", "org.joda.time.*");

        cliOptions.add(new CliOption("modelPackage", "package for generated models"));
        cliOptions.add(new CliOption("apiPackage", "package for generated api classes"));

        outputFolder = "generated-code/java";
        modelTemplateFiles.put("model.mustache", ".java");
        apiTemplateFiles.put("api.mustache", ".java");
        templateDir = "ExplicitJava";
        apiPackage = "io.swagger.client.api";
        modelPackage = "io.swagger.client.model";

        reservedWords = new HashSet<>(
                Arrays.asList(
                        "abstract", "continue", "for", "new", "switch", "assert",
                        "default", "if", "package", "synchronized", "boolean", "do", "goto", "private",
                        "this", "break", "double", "implements", "protected", "throw", "byte", "else",
                        "import", "public", "throws", "case", "enum", "instanceof", "return", "transient",
                        "catch", "extends", "int", "short", "try", "char", "final", "interface", "static",
                        "void", "class", "finally", "long", "strictfp", "volatile", "const", "float",
                        "native", "super", "while")
        );

        languageSpecificPrimitives = new HashSet<>(
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

    private static String camelize(String word)
    {
        return camelize(word, false);
    }

    private static String camelize(String word, boolean lowercaseFirstLetter)
    {
        // Replace all slashes with dots (package separator)
        Pattern p = Pattern.compile("\\/(.?)");
        Matcher m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst("." + m.group(1)/*.toUpperCase()*/);
            m = p.matcher(word);
        }

        // case out dots
        String[] parts = word.split("\\.");
        StringBuilder f = new StringBuilder();
        for (String z : parts) {
            if (z.length() > 0) {
                f.append(Character.toUpperCase(z.charAt(0))).append(z.substring(1));
            }
        }
        word = f.toString();

        m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst("" + Character.toUpperCase(m.group(1).charAt(0)) + m.group(1).substring(1)/*.toUpperCase()*/);
            m = p.matcher(word);
        }

        // Uppercase the class name.
        p = Pattern.compile("(\\.?)(\\w)([^\\.]*)$");
        m = p.matcher(word);
        if (m.find()) {
            String rep = m.group(1) + m.group(2).toUpperCase() + m.group(3);
            rep = rep.replaceAll("\\$", "\\\\\\$");
            word = m.replaceAll(rep);
        }

        // Replace two underscores with $ to support inner classes.
        p = Pattern.compile("(__)(.)");
        m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst("\\$" + m.group(2).toUpperCase());
            m = p.matcher(word);
        }

        // Remove all underscores
        p = Pattern.compile("(_)(.)");
        m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst(m.group(2).toUpperCase());
            m = p.matcher(word);
        }

        if (lowercaseFirstLetter) {
            word = word.substring(0, 1).toLowerCase() + word.substring(1);
        }

        return word;
    }

    @Override
    public List<CliOption> cliOptions()
    {
        return cliOptions;
    }

    @Override
    public void processOpts()
    {
        if (additionalProperties.containsKey("templateDir")) {
            this.setTemplateDir((String) additionalProperties.get("templateDir"));
        }

        if (additionalProperties.containsKey("modelPackage")) {
            this.setModelPackage((String) additionalProperties.get("modelPackage"));
        }

        if (additionalProperties.containsKey("apiPackage")) {
            this.setApiPackage((String) additionalProperties.get("apiPackage"));
        }

        if (additionalProperties.containsKey("invokerPackage")) {
            this.setInvokerPackage((String) additionalProperties.get("invokerPackage"));
        }
        else {
            //not set, use default to be passed to template
            additionalProperties.put("invokerPackage", invokerPackage);
        }

        if (additionalProperties.containsKey("groupId")) {
            this.setGroupId((String) additionalProperties.get("groupId"));
        }
        else {
            //not set, use to be passed to template
            additionalProperties.put("groupId", groupId);
        }

        if (additionalProperties.containsKey("artifactId")) {
            this.setArtifactId((String) additionalProperties.get("artifactId"));
        }
        else {
            //not set, use to be passed to template
            additionalProperties.put("artifactId", artifactId);
        }

        if (additionalProperties.containsKey("artifactVersion")) {
            this.setArtifactVersion((String) additionalProperties.get("artifactVersion"));
        }
        else {
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

    private void setInvokerPackage(String invokerPackage)
    {
        this.invokerPackage = invokerPackage;
    }

    private void setGroupId(String groupId)
    {
        this.groupId = groupId;
    }

    private void setArtifactId(String artifactId)
    {
        this.artifactId = artifactId;
    }

    private void setArtifactVersion(String artifactVersion)
    {
        this.artifactVersion = artifactVersion;
    }

    private void setSourceFolder(String sourceFolder)
    {
        this.sourceFolder = sourceFolder;
    }

    // override with any special post-processing
    @Override
    public Map<String, Object> postProcessModels(Map<String, Object> objs)
    {
        return objs;
    }

    // override with any special post-processing
    @Override
    public Map<String, Object> postProcessOperations(Map<String, Object> objs)
    {
        return objs;
    }

    // override with any special post-processing
    @Override
    public Map<String, Object> postProcessSupportingFileData(Map<String, Object> objs)
    {
        return objs;
    }

    // override with any special handling of the entire swagger spec
    @Override
    public void processSwagger(Swagger swagger)
    {
    }

    // override with any special text escaping logic
    @Override
    public String escapeText(String input)
    {
        if (input != null) {
            String output = input.replaceAll("\n", "\\\\n");
            output = output.replace("\"", "\\\"");
            return output;
        }
        return input;
    }

    @Override
    public Set<String> defaultIncludes()
    {
        return defaultIncludes;
    }

    @Override
    public Map<String, String> typeMapping()
    {
        return typeMapping;
    }

    @Override
    public Map<String, String> instantiationTypes()
    {
        return instantiationTypes;
    }

    @Override
    public Set<String> reservedWords()
    {
        return reservedWords;
    }

    private Set<String> languageSpecificPrimitives()
    {
        return languageSpecificPrimitives;
    }

    @Override
    public Map<String, String> importMapping()
    {
        return importMapping;
    }

    @Override
    public String modelPackage()
    {
        return modelPackage;
    }

    @Override
    public String apiPackage()
    {
        return apiPackage;
    }

    @Override
    public String fileSuffix()
    {
        return fileSuffix;
    }

    @Override
    public String templateDir()
    {
        return templateDir;
    }

    @Override
    public Map<String, String> apiTemplateFiles()
    {
        return apiTemplateFiles;
    }

    @Override
    public Map<String, String> modelTemplateFiles()
    {
        return modelTemplateFiles;
    }

    @Override
    public String apiFileFolder()
    {
        return outputFolder + "/" + sourceFolder + "/" + apiPackage().replace('.', File.separatorChar);
    }

    @Override
    public String modelFileFolder()
    {
        return outputFolder + "/" + sourceFolder + "/" + modelPackage().replace('.', File.separatorChar);
    }

    @Override
    public Map<String, Object> additionalProperties()
    {
        return additionalProperties;
    }

    @Override
    public List<SupportingFile> supportingFiles()
    {
        return supportingFiles;
    }

    @Override
    public String outputFolder()
    {
        return outputFolder;
    }

    @Override
    public String getOutputDir()
    {
        return outputFolder();
    }

    @Override
    public void setOutputDir(String dir)
    {
        this.outputFolder = dir;
    }

    private void setTemplateDir(String templateDir)
    {
        this.templateDir = templateDir;
    }

    private void setModelPackage(String modelPackage)
    {
        this.modelPackage = modelPackage;
    }

    private void setApiPackage(String apiPackage)
    {
        this.apiPackage = apiPackage;
    }

    @Override
    public String toApiFilename(String name)
    {
        return toApiName(name);
    }

    @Override
    public String toApiVarName(String name)
    {
        return snakeCase(name);
    }

    @Override
    public String toModelFilename(String name)
    {
        // should be the same as the model name
        return toModelName(name);
    }

    private String toOperationId(String operationId)
    {
        if (reservedWords.contains(operationId)) {
            throw new RuntimeException(operationId + " (reserved word) cannot be used as method name");
        }

        return camelize(operationId, true);
    }

    private String toVarName(String name)
    {
        // replace - with _ e.g. created-at => created_at
        name = name.replaceAll("-", "_");

        // if it's all upper case, do nothing
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
    public String toParamName(String name)
    {
        // should be the same as variable name
        return toVarName(name);
    }

    private String toEnumName(CodegenProperty property)
    {
        return StringUtils.capitalize(property.name) + "Enum";
    }

    @Override
    public String escapeReservedWord(String name)
    {
        return "_" + name;
    }

    @Override
    public String toModelImport(String name)
    {
        if ("".equals(modelPackage())) {
            return name;
        }
        else {
            return modelPackage() + "." + name;
        }
    }

    @Override
    public String toApiImport(String name)
    {
        return apiPackage() + "." + name;
    }

    @Override
    public CodegenType getTag()
    {
        return CodegenType.CLIENT;
    }

    @Override
    public String getName()
    {
        return "java";
    }

    @Override
    public String getHelp()
    {
        return "Generates a Java client library.";
    }

    @Override
    public String generateExamplePath(String path, Operation operation)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(path);

        if (operation.getParameters() != null) {
            int count = 0;

            for (Parameter param : operation.getParameters()) {
                if (param instanceof QueryParameter) {
                    StringBuilder paramPart = new StringBuilder();
                    QueryParameter qp = (QueryParameter) param;

                    if (count == 0) {
                        paramPart.append("?");
                    }
                    else {
                        paramPart.append(",");
                    }
                    count += 1;
                    if (!param.getRequired()) {
                        paramPart.append("[");
                    }
                    paramPart.append(param.getName()).append("=");
                    paramPart.append("{");
                    if (qp.getCollectionFormat() != null) {
                        paramPart.append(param.getName() + "1");
                        if ("csv".equals(qp.getCollectionFormat())) {
                            paramPart.append(",");
                        }
                        else if ("pipes".equals(qp.getCollectionFormat())) {
                            paramPart.append("|");
                        }
                        else if ("tsv".equals(qp.getCollectionFormat())) {
                            paramPart.append("\t");
                        }
                        else if ("multi".equals(qp.getCollectionFormat())) {
                            paramPart.append("&").append(param.getName()).append("=");
                            paramPart.append(param.getName() + "2");
                        }
                    }
                    else {
                        paramPart.append(param.getName());
                    }
                    paramPart.append("}");
                    if (!param.getRequired()) {
                        paramPart.append("]");
                    }
                    sb.append(paramPart.toString());
                }
            }
        }

        return sb.toString();
    }

    private String toInstantiationType(Property p)
    {
        if (p instanceof MapProperty) {
            MapProperty ap = (MapProperty) p;
            String inner = getSwaggerType(ap.getAdditionalProperties());
            return instantiationTypes.get("map") + "<String, " + inner + ">";
        }
        else if (p instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty) p;
            String inner = getSwaggerType(ap.getItems());
            return instantiationTypes.get("array") + "<" + inner + ">";
        }
        else {
            return null;
        }
    }

    private String toDefaultValue(Property p)
    {
        if (p instanceof StringProperty) {
            return "null";
        }
        else if (p instanceof BooleanProperty) {
            return "null";
        }
        else if (p instanceof DateProperty) {
            return "null";
        }
        else if (p instanceof DateTimeProperty) {
            return "null";
        }
        else if (p instanceof DoubleProperty) {
            DoubleProperty dp = (DoubleProperty) p;
            if (dp.getDefault() != null) {
                return dp.getDefault().toString();
            }
            return "null";
        }
        else if (p instanceof FloatProperty) {
            FloatProperty dp = (FloatProperty) p;
            if (dp.getDefault() != null) {
                return dp.getDefault().toString();
            }
            return "null";
        }
        else if (p instanceof IntegerProperty) {
            IntegerProperty dp = (IntegerProperty) p;
            if (dp.getDefault() != null) {
                return dp.getDefault().toString();
            }
            return "null";
        }
        else if (p instanceof LongProperty) {
            LongProperty dp = (LongProperty) p;
            if (dp.getDefault() != null) {
                return dp.getDefault().toString();
            }
            return "null";
        }
        else if (p instanceof MapProperty) {
            MapProperty ap = (MapProperty) p;
            String inner = getSwaggerType(ap.getAdditionalProperties());
            return "new HashMap<String, " + inner + ">() ";
        }
        else if (p instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty) p;
            String inner = getSwaggerType(ap.getItems());
            return "new ArrayList<" + inner + ">() ";
        }
        else {
            return "null";
        }
    }

    private String getSwaggerType(Property p)
    {
        if (p instanceof RefProperty) {
            RefProperty r = (RefProperty) p;
            String datatype = r.get$ref();
            if (datatype.indexOf("#/definitions/") == 0) {
                datatype = datatype.substring("#/definitions/".length());
            }
            return modelPackage + "." + datatype;
        }
        String swaggerType = _getSwaggerType(p);
        String type = null;
        if (typeMapping.containsKey(swaggerType)) {
            type = typeMapping.get(swaggerType);
            if (languageSpecificPrimitives.contains(type)) {
                return toModelName(type);
            }
        }
        else {
            type = swaggerType;
        }
        return toModelName(type);
    }

    /**
     * returns the swagger type for the property
     **/
    private String _getSwaggerType(Property p)
    {
        String datatype = null;
        if (p instanceof StringProperty) {
            datatype = "string";
        }
        else if (p instanceof BooleanProperty) {
            datatype = "boolean";
        }
        else if (p instanceof DateProperty) {
            datatype = "date";
        }
        else if (p instanceof DateTimeProperty) {
            datatype = "DateTime";
        }
        else if (p instanceof DoubleProperty) {
            datatype = "double";
        }
        else if (p instanceof FloatProperty) {
            datatype = "float";
        }
        else if (p instanceof IntegerProperty) {
            datatype = "integer";
        }
        else if (p instanceof LongProperty) {
            datatype = "long";
        }
        else if (p instanceof MapProperty) {
            datatype = "map";
        }
        else if (p instanceof DecimalProperty) {
            datatype = "number";
        }
        else if (p instanceof RefProperty) {
            RefProperty r = (RefProperty) p;
            datatype = r.get$ref();
            if (datatype.indexOf("#/definitions/") == 0) {
                datatype = datatype.substring("#/definitions/".length());
            }
        }
        else {
            if (p != null) {
                datatype = p.getType();
            }
        }
        return datatype;
    }

    private String snakeCase(String name)
    {
        return (name.length() > 0) ? (Character.toLowerCase(name.charAt(0)) + name.substring(1)) : "";
    }

    private String initialCaps(String name)
    {
        return StringUtils.capitalize(name);
    }

    @Override
    public String getTypeDeclaration(String name)
    {
        return name;
    }

    @Override
    public String getTypeDeclaration(Property p)
    {
        if (p instanceof ArrayProperty) {
            ArrayProperty ap = (ArrayProperty) p;
            Property inner = ap.getItems();
            return getSwaggerType(p) + "<" + getTypeDeclaration(inner) + ">";
        }
        else if (p instanceof MapProperty) {
            MapProperty mp = (MapProperty) p;
            Property inner = mp.getAdditionalProperties();

            return getSwaggerType(p) + "<String, " + getTypeDeclaration(inner) + ">";
        }
        String swaggerType = getSwaggerType(p);
        if (typeMapping.containsKey(swaggerType)) {
            return typeMapping.get(swaggerType);
        }
        return swaggerType;
    }

    @Override
    public String toApiName(String name)
    {
        if (name.length() == 0) {
            return "DefaultApi";
        }
        return initialCaps(name) + "Api";
    }

    @Override
    public String toModelName(String name)
    {
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
    public CodegenModel fromModel(String name, Model model)
    {
        CodegenModel m = CodegenModelFactory.newInstance(CodegenModelType.MODEL);
        if (reservedWords.contains(name)) {
            m.name = escapeReservedWord(name);
        }
        else {
            m.name = name;
        }
        m.description = escapeText(model.getDescription());
        m.classname = toModelName(name);
        m.classVarName = toVarName(name);
        m.modelJson = Json.pretty(model);
        m.externalDocs = model.getExternalDocs();
        if (model instanceof ArrayModel) {
            ArrayModel am = (ArrayModel) model;
            ArrayProperty arrayProperty = new ArrayProperty(am.getItems());
            addParentContainer(m, name, arrayProperty);
        }
        else if (model instanceof RefModel) {
            // TODO
        }
        else if (model instanceof ComposedModel) {
            final ComposedModel composed = (ComposedModel) model;
            final RefModel parent = (RefModel) composed.getParent();
            final String parentModel = toModelName(parent.getSimpleRef());
            m.parent = parentModel;
            addImport(m, parentModel);
            final ModelImpl child = (ModelImpl) composed.getChild();
            addVars(m, child.getProperties(), child.getRequired());
        }
        else {
            ModelImpl impl = (ModelImpl) model;
            if (impl.getAdditionalProperties() != null) {
                MapProperty mapProperty = new MapProperty(impl.getAdditionalProperties());
                addParentContainer(m, name, mapProperty);
            }
            addVars(m, impl.getProperties(), impl.getRequired());
        }
        return m;
    }

    private String getterAndSetterCapitalize(String name)
    {
        if (name == null || name.length() == 0) {
            return name;
        }

        return camelize(toVarName(name));
    }

    private CodegenProperty fromProperty(String name, Property p)
    {
        if (p == null) {
            LOGGER.error("unexpected missing property for name " + null);
            return null;
        }
        CodegenProperty property = CodegenModelFactory.newInstance(CodegenModelType.PROPERTY);

        property.name = toVarName(name);
        property.baseName = name;
        property.description = escapeText(p.getDescription());
        property.getter = "get" + getterAndSetterCapitalize(name);
        property.setter = "set" + getterAndSetterCapitalize(name);
        property.example = p.getExample();
        property.defaultValue = toDefaultValue(p);
        property.jsonSchema = Json.pretty(p);

        String type = getSwaggerType(p);
        if (p instanceof AbstractNumericProperty) {
            AbstractNumericProperty np = (AbstractNumericProperty) p;
            property.minimum = np.getMinimum();
            property.maximum = np.getMaximum();
            property.exclusiveMinimum = np.getExclusiveMinimum();
            property.exclusiveMaximum = np.getExclusiveMaximum();

            // legacy support
            Map<String, Object> allowableValues = new HashMap<>();
            if (np.getMinimum() != null) {
                allowableValues.put("min", np.getMinimum());
            }
            if (np.getMaximum() != null) {
                allowableValues.put("max", np.getMaximum());
            }
            property.allowableValues = allowableValues;
        }

        if (p instanceof StringProperty) {
            StringProperty sp = (StringProperty) p;
            property.maxLength = sp.getMaxLength();
            property.minLength = sp.getMinLength();
            property.pattern = sp.getPattern();
            if (sp.getEnum() != null) {
                List<String> _enum = sp.getEnum();
                property._enum = _enum;
                property.isEnum = true;

                // legacy support
                Map<String, Object> allowableValues = new HashMap<>();
                allowableValues.put("values", _enum);
                property.allowableValues = allowableValues;
            }
        }

        property.datatype = getTypeDeclaration(p);

        // this can cause issues for clients which don't support enums
        if (property.isEnum) {
            property.datatypeWithEnum = toEnumName(property);
        }
        else {
            property.datatypeWithEnum = property.datatype;
        }

        property.baseType = getSwaggerType(p);

        if (p instanceof ArrayProperty) {
            property.isContainer = true;
            property.containerType = "array";
            ArrayProperty ap = (ArrayProperty) p;
            CodegenProperty cp = fromProperty("inner", ap.getItems());
            if (cp == null) {
                LOGGER.warn("skipping invalid property " + Json.pretty(p));
            }
            else {
                property.baseType = getSwaggerType(p);
                if (!languageSpecificPrimitives.contains(cp.baseType)) {
                    property.complexType = cp.baseType;
                }
                else {
                    property.isPrimitiveType = true;
                }
            }
        }
        else if (p instanceof MapProperty) {
            property.isContainer = true;
            property.containerType = "map";
            MapProperty ap = (MapProperty) p;
            CodegenProperty cp = fromProperty("inner", ap.getAdditionalProperties());

            property.baseType = getSwaggerType(p);
            if (!languageSpecificPrimitives.contains(cp.baseType)) {
                property.complexType = cp.baseType;
            }
            else {
                property.isPrimitiveType = true;
            }
        }
        else {
            setNonArrayMapProperty(property, type);
        }
        return property;
    }

    private void setNonArrayMapProperty(CodegenProperty property, String type)
    {
        property.isNotContainer = true;
        if (languageSpecificPrimitives().contains(type)) {
            property.isPrimitiveType = true;
        }
        else {
            property.complexType = property.baseType;
        }
    }

    private Response findMethodResponse(Map<String, Response> responses)
    {

        String code = null;
        for (String responseCode : responses.keySet()) {
            if (responseCode.startsWith("2") || responseCode.equals("default")) {
                if (code == null || code.compareTo(responseCode) > 0) {
                    code = responseCode;
                }
            }
        }
        if (code == null) {
            return null;
        }
        return responses.get(code);
    }

    @Override
    public CodegenOperation fromOperation(String path, String httpMethod, Operation operation, Map<String, Model> definitions)
    {
        CodegenOperation op = CodegenModelFactory.newInstance(CodegenModelType.OPERATION);
        Set<String> imports = new HashSet<>();

        String operationId = operation.getOperationId();
        if (operationId == null) {
            String tmpPath = path;
            tmpPath = tmpPath.replaceAll("\\{", "");
            tmpPath = tmpPath.replaceAll("\\}", "");
            String[] parts = (tmpPath + "/" + httpMethod).split("/");
            StringBuilder builder = new StringBuilder();
            if ("/".equals(tmpPath)) {
                // must be root tmpPath
                builder.append("root");
            }
            for (String part1 : parts) {
                String part = part1;
                if (part.length() > 0) {
                    if (builder.toString().length() == 0) {
                        part = Character.toLowerCase(part.charAt(0)) + part.substring(1);
                    }
                    else {
                        part = initialCaps(part);
                    }
                    builder.append(part);
                }
            }
            operationId = builder.toString();
            LOGGER.warn("generated operationId " + operationId);
        }
        operationId = removeNonNameElementToCamelCase(operationId);
        op.path = path;
        op.operationId = toOperationId(operationId);
        op.summary = escapeText(operation.getSummary());
        op.notes = escapeText(operation.getDescription());
        op.tags = operation.getTags();

        if (operation.getConsumes() != null && operation.getConsumes().size() > 0) {
            List<Map<String, String>> c = new ArrayList<>();
            int count = 0;
            for (String key : operation.getConsumes()) {
                Map<String, String> mediaType = new HashMap<>();
                mediaType.put("mediaType", key);
                count += 1;
                if (count < operation.getConsumes().size()) {
                    mediaType.put("hasMore", "true");
                }
                else {
                    mediaType.put("hasMore", null);
                }
                c.add(mediaType);
            }
            op.consumes = c;
            op.hasConsumes = true;
        }

        if (operation.getProduces() != null && operation.getProduces().size() > 0) {
            List<Map<String, String>> c = new ArrayList<>();
            int count = 0;
            for (String key : operation.getProduces()) {
                Map<String, String> mediaType = new HashMap<>();
                mediaType.put("mediaType", key);
                count += 1;
                if (count < operation.getProduces().size()) {
                    mediaType.put("hasMore", "true");
                }
                else {
                    mediaType.put("hasMore", null);
                }
                c.add(mediaType);
            }
            op.produces = c;
            op.hasProduces = true;
        }

        if (operation.getResponses() != null && !operation.getResponses().isEmpty()) {
            Response methodResponse = findMethodResponse(operation.getResponses());

            for (Map.Entry<String, Response> entry : operation.getResponses().entrySet()) {
                Response response = entry.getValue();
                CodegenResponse r = fromResponse(entry.getKey(), response);
                r.hasMore = true;
                if (r.baseType != null &&
                        !defaultIncludes.contains(r.baseType) &&
                        !languageSpecificPrimitives.contains(r.baseType)) {
                    imports.add(r.baseType);
                }
                r.isDefault = response == methodResponse;
                op.responses.add(r);
            }
            op.responses.get(op.responses.size() - 1).hasMore = false;

            if (methodResponse != null) {
                if (methodResponse.getSchema() != null) {
                    CodegenProperty cm = fromProperty("response", methodResponse.getSchema());

                    Property responseProperty = methodResponse.getSchema();

                    if (responseProperty instanceof ArrayProperty) {
                        ArrayProperty ap = (ArrayProperty) responseProperty;
                        CodegenProperty innerProperty = fromProperty("response", ap.getItems());
                        op.returnBaseType = innerProperty.baseType;
                    }
                    else {
                        if (cm.complexType != null) {
                            op.returnBaseType = cm.complexType;
                        }
                        else {
                            op.returnBaseType = cm.baseType;
                        }
                    }
                    op.examples = new ExampleGenerator(definitions).generate(methodResponse.getExamples(), operation.getProduces(), responseProperty);
                    op.defaultResponse = toDefaultValue(responseProperty);
                    op.returnType = cm.datatype;
                    if (cm.isContainer != null) {
                        op.returnContainer = cm.containerType;
                        if ("map".equals(cm.containerType)) {
                            op.isMapContainer = Boolean.TRUE;
                        }
                        else if ("list".equalsIgnoreCase(cm.containerType)) {
                            op.isListContainer = Boolean.TRUE;
                        }
                        else if ("array".equalsIgnoreCase(cm.containerType)) {
                            op.isListContainer = Boolean.TRUE;
                        }
                    }
                    else {
                        op.returnSimpleType = true;
                    }
                    if (languageSpecificPrimitives().contains(op.returnBaseType) || op.returnBaseType == null) {
                        op.returnTypeIsPrimitive = true;
                    }
                }
                addHeaders(methodResponse, op.responseHeaders);
            }
        }

        List<Parameter> parameters = operation.getParameters();
        CodegenParameter bodyParam = null;
        List<CodegenParameter> allParams = new ArrayList<>();
        List<CodegenParameter> bodyParams = new ArrayList<>();
        List<CodegenParameter> pathParams = new ArrayList<>();
        List<CodegenParameter> queryParams = new ArrayList<>();
        List<CodegenParameter> headerParams = new ArrayList<>();
        List<CodegenParameter> cookieParams = new ArrayList<>();
        List<CodegenParameter> formParams = new ArrayList<>();

        if (parameters != null) {
            for (Parameter param : parameters) {
                CodegenParameter p = fromParameter(param, imports);
                allParams.add(p);
                if (param instanceof QueryParameter) {
                    p.isQueryParam = true;
                    queryParams.add(p.copy());
                }
                else if (param instanceof PathParameter) {
                    p.required = true;
                    p.isPathParam = true;
                    pathParams.add(p.copy());
                }
                else if (param instanceof HeaderParameter) {
                    p.isHeaderParam = true;
                    headerParams.add(p.copy());
                }
                else if (param instanceof CookieParameter) {
                    p.isCookieParam = true;
                    cookieParams.add(p.copy());
                }
                else if (param instanceof BodyParameter) {
                    p.isBodyParam = true;
                    bodyParam = p;
                    bodyParams.add(p.copy());
                }
                else if (param instanceof FormParameter) {
                    if ("file".equalsIgnoreCase(((FormParameter) param).getType())) {
                        p.isFile = true;
                    }
                    else {
                        p.notFile = true;
                    }
                    p.isFormParam = true;
                    formParams.add(p.copy());
                }
            }
        }
        for (String i : imports) {
            if (!defaultIncludes.contains(i) && !languageSpecificPrimitives.contains(i)) {
                op.imports.add(i);
            }
        }
        op.bodyParam = bodyParam;
        op.httpMethod = httpMethod.toUpperCase();
        op.allParams = addHasMore(allParams);
        op.bodyParams = addHasMore(bodyParams);
        op.pathParams = addHasMore(pathParams);
        op.queryParams = addHasMore(queryParams);
        op.headerParams = addHasMore(headerParams);
        // op.cookieParams = cookieParams;
        op.formParams = addHasMore(formParams);
        // legacy support
        op.nickname = op.operationId;

        if (op.allParams.size() > 0) {
            op.hasParams = true;
        }
        op.externalDocs = operation.getExternalDocs();

        return op;
    }

    private CodegenResponse fromResponse(String responseCode, Response response)
    {
        CodegenResponse r = CodegenModelFactory.newInstance(CodegenModelType.RESPONSE);
        if ("default".equals(responseCode)) {
            r.code = "0";
        }
        else {
            r.code = responseCode;
        }
        r.message = response.getDescription();
        r.schema = response.getSchema();
        r.examples = toExamples(response.getExamples());
        r.jsonSchema = Json.pretty(response);
        addHeaders(response, r.headers);

        if (r.schema != null) {
            Property responseProperty = response.getSchema();
            responseProperty.setRequired(true);
            CodegenProperty cm = fromProperty("response", responseProperty);

            if (responseProperty instanceof ArrayProperty) {
                ArrayProperty ap = (ArrayProperty) responseProperty;
                CodegenProperty innerProperty = fromProperty("response", ap.getItems());
                r.baseType = innerProperty.baseType;
            }
            else {
                if (cm.complexType != null) {
                    r.baseType = cm.complexType;
                }
                else {
                    r.baseType = cm.baseType;
                }
            }
            r.dataType = cm.datatype;
            if (cm.isContainer != null) {
                r.simpleType = false;
                r.containerType = cm.containerType;
                r.isMapContainer = "map".equals(cm.containerType);
                r.isListContainer = "list".equals(cm.containerType);
            }
            else {
                r.simpleType = true;
            }
            r.primitiveType = (r.baseType == null || languageSpecificPrimitives().contains(r.baseType));
        }
        if (r.baseType == null) {
            r.isMapContainer = false;
            r.isListContainer = false;
            r.primitiveType = true;
            r.simpleType = true;
        }
        return r;
    }

    private CodegenParameter fromParameter(Parameter param, Set<String> imports)
    {
        CodegenParameter p = CodegenModelFactory.newInstance(CodegenModelType.PARAMETER);
        p.baseName = param.getName();
        p.description = escapeText(param.getDescription());
        if (param.getRequired()) {
            p.required = param.getRequired();
        }
        p.jsonSchema = Json.pretty(param);

        // move the defaultValue for headers, forms and params
        if (param instanceof QueryParameter) {
            p.defaultValue = ((QueryParameter) param).getDefaultValue();
        }
        else if (param instanceof HeaderParameter) {
            p.defaultValue = ((HeaderParameter) param).getDefaultValue();
        }
        else if (param instanceof FormParameter) {
            p.defaultValue = ((FormParameter) param).getDefaultValue();
        }

        if (param instanceof SerializableParameter) {
            SerializableParameter qp = (SerializableParameter) param;
            Property property = null;
            String collectionFormat = null;
            if ("array".equals(qp.getType())) {
                Property inner = qp.getItems();
                if (inner == null) {
                    LOGGER.warn("warning!  No inner type supplied for array parameter \"" + qp.getName() + "\", using String");
                    inner = new StringProperty().description("//TODO automatically added by swagger-codegen");
                }
                property = new ArrayProperty(inner);
                collectionFormat = qp.getCollectionFormat();
                CodegenProperty pr = fromProperty("inner", inner);
                p.baseType = pr.datatype;
                p.isContainer = true;
                imports.add(pr.baseType);
            }
            else if ("object".equals(qp.getType())) {
                Property inner = qp.getItems();
                if (inner == null) {
                    LOGGER.warn("warning!  No inner type supplied for map parameter \"" + qp.getName() + "\", using String");
                    inner = new StringProperty().description("//TODO automatically added by swagger-codegen");
                }
                property = new MapProperty(inner);
                collectionFormat = qp.getCollectionFormat();
                CodegenProperty pr = fromProperty("inner", inner);
                p.baseType = pr.datatype;
                imports.add(pr.baseType);
            }
            else {
                property = PropertyBuilder.build(qp.getType(), qp.getFormat(), null);
            }
            if (property == null) {
                LOGGER.warn("warning!  Property type \"" + qp.getType() + "\" not found for parameter \"" + param.getName() + "\", using String");
                property = new StringProperty().description("//TODO automatically added by swagger-codegen.  Type was " + qp.getType() + " but not supported");
            }
            property.setRequired(param.getRequired());
            CodegenProperty model = fromProperty(qp.getName(), property);
            p.collectionFormat = collectionFormat;
            p.dataType = model.datatype;
            p.paramName = toParamName(qp.getName());

            if (model.complexType != null) {
                imports.add(model.complexType);
            }
        }
        else {
            BodyParameter bp = (BodyParameter) param;
            Model model = bp.getSchema();

            if (model instanceof ModelImpl) {
                ModelImpl impl = (ModelImpl) model;
                CodegenModel cm = fromModel(bp.getName(), impl);
                if (cm.emptyVars != null && cm.emptyVars == false) {
                    p.dataType = getTypeDeclaration(cm.classname);
                    imports.add(p.dataType);
                }
                else {
                    // TODO: missing format, so this will not always work
                    Property prop = PropertyBuilder.build(impl.getType(), null, null);
                    prop.setRequired(bp.getRequired());
                    CodegenProperty cp = fromProperty("property", prop);
                    if (cp != null) {
                        p.dataType = cp.datatype;
                    }
                }
            }
            else if (model instanceof ArrayModel) {
                // to use the built-in model parsing, we unwrap the ArrayModel
                // and get a single property from it
                ArrayModel impl = (ArrayModel) model;
                CodegenModel cm = fromModel(bp.getName(), impl);
                // get the single property
                ArrayProperty ap = new ArrayProperty().items(impl.getItems());
                ap.setRequired(param.getRequired());
                CodegenProperty cp = fromProperty("inner", ap);
                if (cp.complexType != null) {
                    imports.add(cp.complexType);
                }
                imports.add(cp.baseType);
                p.dataType = cp.datatype;
                p.isContainer = true;
            }
            else {
                Model sub = bp.getSchema();
                if (sub instanceof RefModel) {
                    String name = ((RefModel) sub).getSimpleRef();
                    if (typeMapping.containsKey(name)) {
                        name = typeMapping.get(name);
                    }
                    else {
                        name = toModelName(name);
                        if (defaultIncludes.contains(name)) {
                            imports.add(name);
                        }
                        imports.add(name);
                        name = getTypeDeclaration(name);
                    }
                    p.dataType = name;
                }
            }
            p.paramName = toParamName(bp.getName());
        }
        return p;
    }

    @Override
    public List<CodegenSecurity> fromSecurity(Map<String, SecuritySchemeDefinition> schemes)
    {
        if (schemes == null) {
            return null;
        }

        List<CodegenSecurity> secs = new ArrayList<>(schemes.size());
        for (Iterator<Map.Entry<String, SecuritySchemeDefinition>> it = schemes.entrySet().iterator(); it.hasNext(); ) {
            final Map.Entry<String, SecuritySchemeDefinition> entry = it.next();
            final SecuritySchemeDefinition schemeDefinition = entry.getValue();

            CodegenSecurity sec = CodegenModelFactory.newInstance(CodegenModelType.SECURITY);
            sec.name = entry.getKey();
            sec.type = schemeDefinition.getType();

            if (schemeDefinition instanceof ApiKeyAuthDefinition) {
                final ApiKeyAuthDefinition apiKeyDefinition = (ApiKeyAuthDefinition) schemeDefinition;
                sec.isBasic = sec.isOAuth = false;
                sec.isApiKey = true;
                sec.keyParamName = apiKeyDefinition.getName();
                sec.isKeyInHeader = apiKeyDefinition.getIn() == In.HEADER;
                sec.isKeyInQuery = !sec.isKeyInHeader;
            }
            else {
                sec.isKeyInHeader = sec.isKeyInQuery = sec.isApiKey = false;
                sec.isBasic = schemeDefinition instanceof BasicAuthDefinition;
                sec.isOAuth = !sec.isBasic;
            }

            sec.hasMore = it.hasNext();
            secs.add(sec);
        }
        return secs;
    }

    private List<Map<String, Object>> toExamples(Map<String, Object> examples)
    {
        if (examples == null) {
            return null;
        }

        final List<Map<String, Object>> output = new ArrayList<>(examples.size());
        for (Map.Entry<String, Object> entry : examples.entrySet()) {
            final Map<String, Object> kv = new HashMap<>();
            kv.put("contentType", entry.getKey());
            kv.put("example", entry.getValue());
            output.add(kv);
        }
        return output;
    }

    private void addHeaders(Response response, List<CodegenProperty> target)
    {
        if (response.getHeaders() != null) {
            for (Map.Entry<String, Property> headers : response.getHeaders().entrySet()) {
                target.add(fromProperty(headers.getKey(), headers.getValue()));
            }
        }
    }

  /* underscore and camelize are copied from Twitter elephant bird
   * https://github.com/twitter/elephant-bird/blob/master/core/src/main/java/com/twitter/elephantbird/util/Strings.java
   */

    private List<CodegenParameter> addHasMore(List<CodegenParameter> objs)
    {
        if (objs != null) {
            for (int i = 0; i < objs.size(); i++) {
                if (i > 0) {
                    objs.get(i).secondaryParam = true;
                }
                if (i < objs.size() - 1) {
                    objs.get(i).hasMore = true;
                }
            }
        }
        return objs;
    }

    @Override
    public void addOperationToGroup(String tag, String resourcePath, Operation operation, CodegenOperation co, Map<String, List<CodegenOperation>> operations)
    {
        List<CodegenOperation> opList = operations.get(tag);
        if (opList == null) {
            opList = new ArrayList<>();
            operations.put(tag, opList);
        }
        opList.add(co);
        co.baseName = tag;
    }

    private void addParentContainer(CodegenModel m, String name, Property property)
    {
        final CodegenProperty tmp = fromProperty(name, property);
        addImport(m, tmp.complexType);
        m.parent = toInstantiationType(property);
        final String containerType = tmp.containerType;
        final String instantiationType = instantiationTypes.get(containerType);
        if (instantiationType != null) {
            addImport(m, instantiationType);
        }
        final String mappedType = typeMapping.get(containerType);
        if (mappedType != null) {
            addImport(m, mappedType);
        }
    }

    private void addImport(CodegenModel m, String type)
    {
        if (type != null && !languageSpecificPrimitives.contains(type) && !defaultIncludes.contains(type)) {
            m.imports.add(type);
        }
    }

    private void addVars(CodegenModel m, Map<String, Property> properties, Collection<String> required)
    {
        if (properties != null && properties.size() > 0) {
            m.hasVars = true;
            m.hasEnums = false;
            final int totalCount = properties.size();
            final Set<String> mandatory = required == null ? Collections.<String>emptySet() : new HashSet<>(required);
            int count = 0;
            for (Map.Entry<String, Property> entry : properties.entrySet()) {
                final String key = entry.getKey();
                final Property prop = entry.getValue();

                if (prop == null) {
                    LOGGER.warn("null property for " + key);
                }
                else {
                    final CodegenProperty cp = fromProperty(key, prop);
                    cp.required = mandatory.contains(key) ? true : null;
                    if (cp.isEnum) {
                        m.hasEnums = true;
                    }
                    count += 1;
                    if (count != totalCount) {
                        cp.hasMore = true;
                    }
                    if (cp.isContainer != null) {
                        addImport(m, typeMapping.get("array"));
                    }
                    addImport(m, cp.baseType);
                    addImport(m, cp.complexType);
                    m.vars.add(cp);
                }
            }
        }
        else {
            m.emptyVars = true;
        }
    }

    /**
     * Remove characters not suitable for variable or method name from the input and camelize it
     *
     * @param name
     * @return
     */
    private String removeNonNameElementToCamelCase(String name)
    {
        String nonNameElementPattern = "[-_:;#]";
        name = StringUtils.join(Lists.transform(Lists.newArrayList(name.split(nonNameElementPattern)), new Function<String, String>()
        {
            @Nullable
            @Override
            public String apply(String input)
            {
                return StringUtils.capitalize(input);
            }
        }), "");
        if (name.length() > 0) {
            name = name.substring(0, 1).toLowerCase() + name.substring(1);
        }
        return name;
    }

    @Override
    public String apiFilename(String templateName, String tag)
    {
        String suffix = apiTemplateFiles().get(templateName);
        return apiFileFolder() + File.separator + toApiFilename(tag) + suffix;
    }

    @Override
    public boolean shouldOverwrite(String filename)
    {
        return true;
    }
}

