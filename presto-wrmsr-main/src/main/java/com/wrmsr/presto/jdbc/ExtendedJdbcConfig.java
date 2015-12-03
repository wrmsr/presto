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
package com.wrmsr.presto.jdbc;

import io.airlift.configuration.Config;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class ExtendedJdbcConfig
{
    private String driverUrl;

    public String getDriverUrl()
    {
        return driverUrl;
    }

    @Config("driver-url")
    public void setDriverUrl(String driverUrl)
    {
        this.driverUrl = driverUrl;
    }

    private String driverClass;

    public String getDriverClass()
    {
        return driverClass;
    }

    @Config("driver-class")
    public void setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
    }

    private boolean isRemotelyAccessible = true;

    public boolean getIsRemotelyAccessible()
    {
        return isRemotelyAccessible;
    }

    @Config("is-remotely-accessible")
    public void setIsRemotelyAccessible(boolean isRemotelyAccessible)
    {
        this.isRemotelyAccessible = isRemotelyAccessible;
    }
}
