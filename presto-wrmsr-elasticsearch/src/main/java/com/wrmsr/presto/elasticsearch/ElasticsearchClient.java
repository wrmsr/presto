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
package com.wrmsr.presto.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.indices.mapping.GetMapping;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ElasticsearchClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, ElasticsearchTable>>> schemas;

    private final JestClient jestClient;

    @Inject
    public ElasticsearchClient(ElasticsearchConfig config, JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws IOException
    {
        checkNotNull(config, "config is null");
        checkNotNull(catalogCodec, "catalogCodec is null");


        HttpClientConfig.Builder httpClientConfigBuilder = new HttpClientConfig.Builder(
                        Arrays.asList(config.getHttpUri().split(",")));
        if (config.getMaxTotalConnections() != null) {
            httpClientConfigBuilder.maxTotalConnection(config.getMaxTotalConnections());
        }
        if (config.getDiscoveryFrequency() != null) {
            httpClientConfigBuilder.discoveryFrequency(config.getDiscoveryFrequency(), TimeUnit.MILLISECONDS);
        }
        httpClientConfigBuilder.discoveryEnabled(config.getDiscoveryEnabled());
        httpClientConfigBuilder.multiThreaded(config.getMultiThreaded());
        if (config.getConnTimeout() != null) {
            httpClientConfigBuilder.connTimeout(config.getConnTimeout());
        }
        if (config.getReadTimeout() != null) {
            httpClientConfigBuilder.readTimeout(config.getReadTimeout());
        }
        if (config.getMaxConnectionIdleTime() != null) {
            httpClientConfigBuilder.maxConnectionIdleTime(config.getMaxConnectionIdleTime(), TimeUnit.MILLISECONDS);
        }

        JestClientFactory clientFactory = new JestClientFactory();
        clientFactory.setHttpClientConfig(httpClientConfigBuilder.build());
        jestClient = clientFactory.getObject();

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        checkNotNull(schema, "schema is null");
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ElasticsearchTable getTable(String schema, String tableName)
    {
        checkNotNull(schema, "schema is null");
        checkNotNull(tableName, "tableName is null");
        new JestHttpClient();
        Map<String, ElasticsearchTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private Supplier<Map<String, Map<String, ElasticsearchTable>>> schemasSupplier(final JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
    {
        return () -> {
            try {
                return lookupSchemas(catalogCodec);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private Map<String, Map<String, ElasticsearchTable>> lookupSchemas(JsonCodec<Map<String, List<ElasticsearchTable>>> catalogCodec)
            throws Exception
    {
        JestResult result = jestClient.execute(new GetMapping.Builder().build());
        /*
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        Map<String, List<ElasticsearchTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
        */
        throw new IllegalStateException();
    }

    private static Function<List<ElasticsearchTable>, Map<String, ElasticsearchTable>> resolveAndIndexTables(final URI metadataUri)
    {
        return tables -> {
            Iterable<ElasticsearchTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ElasticsearchTable::getName));
        };
    }

    private static Function<ElasticsearchTable, ElasticsearchTable> tableUriResolver(final URI baseUri)
    {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new ElasticsearchTable(table.getName(), table.getColumns(), sources);
        };
    }
}
