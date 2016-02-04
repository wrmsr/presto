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
package com.wrmsr.presto.util;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class Jvm
{
    private Jvm()
    {
    }

    public static void addClasspathUrl(URLClassLoader classLoader, URL url) throws IOException
    {
        try {
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, url);
        }
        catch (Throwable t) {
            t.printStackTrace();
            throw new IOException("Error, could not add URL to system classloader");
        }
    }

    public static void addClasspathUrl(ClassLoader classLoader, URL url) throws IOException
    {
        addClasspathUrl((URLClassLoader) classLoader, url);
    }

    public static File getThisJarFile(Class cls)
    {
        File jar;
        try {
            jar = new File(cls.getProtectionDomain().getCodeSource().getLocation().toURI().getPath());
        }
        catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        checkState(jar.exists());
        return jar;
    }

    public static String formatSystemPropertyJvmOption(String key, String value)
    {
        return "-D" + requireNonNull(key) + "=" + requireNonNull(value);
    }

    public static List<String> formatSystemPropertyJvmOptions(Map<String, String>... propertyMaps)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Map<String, String> properties : Arrays.asList(propertyMaps)) {
            builder.addAll(properties.entrySet().stream().map(e -> formatSystemPropertyJvmOption(e.getKey(), e.getValue())).collect(toImmutableList()));
        }
        return builder.build();
    }
}
