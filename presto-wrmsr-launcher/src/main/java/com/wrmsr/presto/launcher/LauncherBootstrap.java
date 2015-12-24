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
package com.wrmsr.presto.launcher;

import com.wrmsr.presto.util.Repositories;

import java.io.File;

public class LauncherBootstrap
{
    public static void main(String[] args)
            throws Throwable
    {
        if (System.getProperty(Repositories.REPOSITORY_CACHE_PATH_PROPERTY_KEY) == null) {
            File repositoryCachePath = new File(new File(System.getProperty("user.home"), ".m2"), "repository");
            System.setProperty(Repositories.REPOSITORY_CACHE_PATH_PROPERTY_KEY, repositoryCachePath.getAbsolutePath());
        }

        Repositories.setupClassLoaderForModule(LauncherBootstrap.class.getClassLoader(), "presto-wrmsr-launcher");
        Class<?> cls = Class.forName("com.wrmsr.presto.launcher.LauncherMain");
        cls.getDeclaredMethod("main", String[].class).invoke(null, new Object[] {args});
    }
}
