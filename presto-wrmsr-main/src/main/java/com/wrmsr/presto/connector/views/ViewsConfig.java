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
package com.wrmsr.presto.connector.views;

import io.airlift.configuration.Config;

import java.io.File;

public class ViewsConfig
{
    private File directory;

    public File getDirectory()
    {
        return directory;
    }

    @Config("directory")
    public void setDirectory(File directory)
    {
        this.directory = directory;
    }

    private String jdbcConnectionUrl;

    public String getJdbcConnectionUrl()
    {
        return jdbcConnectionUrl;
    }

    @Config("jdbc-connection-url")
    public void setJdbcConnectionUrl(String jdbcConnectionUrl)
    {
        this.jdbcConnectionUrl = jdbcConnectionUrl;
    }

    private String jdbcTableName;

    public String getJdbcTableName()
    {
        return jdbcTableName;
    }

    @Config("jdbc-table-name")
    public void setJdbcTableName(String jdbcTableName)
    {
        this.jdbcTableName = jdbcTableName;
    }

    private String schemaName = "views";

    public String getSchemaName()
    {
        return schemaName;
    }

    @Config("schema-name")
    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }
}
