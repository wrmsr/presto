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

import io.swagger.codegen.ClientOptInput;
import io.swagger.codegen.ClientOpts;
import io.swagger.codegen.CodegenConfig;
import io.swagger.codegen.DefaultGenerator;
import io.swagger.models.Swagger;
import io.swagger.parser.SwaggerParser;
import org.testng.annotations.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.ServiceLoader.load;

public class TestSwaggerPlugin
{
    private static CodegenConfig forName(String name)
    {
        ServiceLoader<CodegenConfig> loader = load(CodegenConfig.class);
        for (CodegenConfig config : loader) {
            if (config.getName().equals(name)) {
                return config;
            }
        }

        // else try to load directly
        try {
            return (CodegenConfig) Class.forName(name).newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Can't load config class with name ".concat(name), e);
        }
    }

    @Test
    public void testStuff()
            throws Throwable
    {
        // javax.tools.JavaCompiler

        ClientOptInput input = new ClientOptInput();

        // if (isNotEmpty(auth)) {
        //     input.setAuth(auth);
        // }
        File root = Files.createTempDirectory(null).toFile();
        root.deleteOnExit(); // FIXME OSX EXECVE SEGFAULT

        CodegenConfig config = new ExplicitJavaClientCodegen();
        config.setOutputDir(root.getAbsolutePath());

        // if (null != templateDir) {
        //     config.additionalProperties().put(TEMPLATE_DIR_PARAM, new File(templateDir).getAbsolutePath());
        // }

        // if (null != configFile) {
        //     Config genConfig = ConfigParser.read(configFile);
        //     if (null != genConfig) {
        //         for (CliOption langCliOption : config.cliOptions()) {
        //             if (genConfig.hasOption(langCliOption.getOpt())) {
        //                 config.additionalProperties().put(langCliOption.getOpt(), genConfig.getOption(langCliOption.getOpt()));
        //             }
        //         }
        //     }
        // }

        input.setConfig(config);

        String spec = System.getProperty("user.home") + "/presto/swagger_1.2/business.json";

        Swagger swagger = new SwaggerParser().read(spec, input.getAuthorizationValues(), true);
        new DefaultGenerator().opts(input.opts(new ClientOpts()).swagger(swagger)).generate();

        // need to :%s/Object/java.lang.Object jesus christ.

        // compile with own classpath in child classloader

        // Prepare source somehow.

        // Save source in .java file.
        File sourceRoot = new File(root, "src/main/java");

        // Compile source file.
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        List<File> sourceFiles =
                Files.walk(sourceRoot.toPath())
                        .filter(Files::isRegularFile)
                        .filter(f -> f.toAbsolutePath().toString().endsWith(".java"))
                        .map(f -> f.toFile())
                        .collect(Collectors.toList());
        for (File sourceFile : sourceFiles) {
            compiler.run(null, null, null, "-source", "1.5", sourceFile.getPath());
        }

        // Load and instantiate compiled class.
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[] {sourceRoot.toURI().toURL()});
        Class<?> cls = Class.forName("test.Test", true, classLoader); // Should print "hello".
        Object instance = cls.newInstance(); // Should print "world".
        System.out.println(instance); // Should print "test.Test@hashcode".
    }
}
