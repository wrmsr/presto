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
package com.wrmsr.presto.jdbc.redshift;

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClientModule;

import java.util.Map;

public class RedshiftClientModule
        extends ExtendedPostgreSqlClientModule
{
    public static final String DEFAULT_DRIVER_URL = "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.1.0001.jar";
    public static final String DEFAULT_DRIVER_CLASS = "com.amazon.redshift.jdbc4.Driver";

    public static Map<String, String> createProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("driver-url", DEFAULT_DRIVER_URL)
                .put("driver-class", DEFAULT_DRIVER_CLASS)
                .build();
    }
}
