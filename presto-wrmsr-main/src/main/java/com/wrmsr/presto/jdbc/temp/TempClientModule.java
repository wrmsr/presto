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
package com.wrmsr.presto.jdbc.temp;

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.jdbc.h2.H2ClientModule;

import java.util.Map;

import static java.lang.String.format;

public class TempClientModule
        extends H2ClientModule
{
    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("connection-url", format("jdbc:h2:mem:test%s;DB_CLOSE_DELAY=-1", System.nanoTime()))
                .put("is-remotely-accessible", "false")
                .put("init", "CREATE SCHEMA temp;")
                .build();
    }
}
