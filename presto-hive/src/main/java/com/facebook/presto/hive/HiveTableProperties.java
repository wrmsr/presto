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
package com.facebook.presto.hive;

import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public class HiveTableProperties
{
    public static final String STORAGE_FORMAT_PROPERTY = "format";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HiveTableProperties(TypeManager typeManager, HiveClientConfig config)
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        STORAGE_FORMAT_PROPERTY,
                        "Hive storage format for the table",
                        VARCHAR,
                        HiveStorageFormat.class,
                        config.getHiveStorageFormat(),
                        false,
                        value -> HiveStorageFormat.valueOf(((String) value).toUpperCase(ENGLISH))),
                new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        typeManager.getParameterizedType(ARRAY, ImmutableList.of(VARCHAR.getTypeSignature()), ImmutableList.of()),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((List<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(Collectors.toList()))));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static HiveStorageFormat getHiveStorageFormat(Map<String, Object> tableProperties)
    {
        return (HiveStorageFormat) tableProperties.get(STORAGE_FORMAT_PROPERTY);
    }

    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(PARTITIONED_BY_PROPERTY);
    }
}
