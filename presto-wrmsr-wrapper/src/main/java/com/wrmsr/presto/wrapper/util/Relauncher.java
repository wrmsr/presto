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
package com.wrmsr.presto.wrapper.util;

import io.airlift.units.DataSize;
import jnr.posix.POSIX;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;


public class Relauncher
{
    public static void exec() throws Exception
    {
        POSIX posix = POSIXUtils.getPOSIX();

        System.out.println(posix.libc().getpid());
        posix.libc().execv("/usr/bin/vim");
    }

    public static void main2(String[] args) throws Exception
    {
        List<Map.Entry> systemProperties = new ArrayList<>(System.getProperties().entrySet());
        systemProperties.sort(new Comparator<Map.Entry>()
        {
            @Override
            public int compare(Map.Entry o1, Map.Entry o2)
            {
                return ((String) o1.getKey()).compareTo((String) o2.getKey());
            }
        });
        for (Map.Entry entry : systemProperties) {
            System.out.println(entry);
        }
        exec();
    }

    public static void main(String[] args) throws Throwable
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();
        for (String s : arguments) {
            System.out.println(s);
        }
    }
}
