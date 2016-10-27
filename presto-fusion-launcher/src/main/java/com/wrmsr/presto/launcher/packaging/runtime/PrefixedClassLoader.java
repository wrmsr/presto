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
package com.wrmsr.presto.launcher.packaging.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

public class PrefixedClassLoader
        extends ClassLoader
{
    public PrefixedClassLoader(ClassLoader parent)
    {
        super(parent);
    }

    @Override
    public Class<?> loadClass(String name)
            throws ClassNotFoundException
    {
        return super.loadClass(name);
    }

    @Override
    public URL getResource(String name)
    {
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name)
            throws IOException
    {
        return super.getResources(name);
    }

    @Override
    public InputStream getResourceAsStream(String name)
    {
        return super.getResourceAsStream(name);
    }

    @Override
    public void setDefaultAssertionStatus(boolean enabled)
    {
        super.setDefaultAssertionStatus(enabled);
    }

    @Override
    public void setPackageAssertionStatus(String packageName, boolean enabled)
    {
        super.setPackageAssertionStatus(packageName, enabled);
    }

    @Override
    public void setClassAssertionStatus(String className, boolean enabled)
    {
        super.setClassAssertionStatus(className, enabled);
    }

    @Override
    public void clearAssertionStatus()
    {
        super.clearAssertionStatus();
    }
}
