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
package com.wrmsr.presto.wrapper;

import com.wrmsr.presto.util.Repositories;

public class PrestoWrapperBootstrap
{
    public static void main(String[] args) throws Throwable
    {
        Repositories.setupClassLoaderForModule(PrestoWrapperBootstrap.class.getClassLoader(), "presto-wrmsr-wrapper");
        Class<?> cls = Class.forName("com.wrmsr.presto.wrapper.PrestoWrapperMain");
        cls.getDeclaredMethod("main", String[].class).invoke(null, new Object[]{args});
    }
}
