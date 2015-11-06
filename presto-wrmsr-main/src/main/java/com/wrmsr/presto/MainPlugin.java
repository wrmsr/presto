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
package com.wrmsr.presto;

// import com.facebook.presto.metadata.FunctionFactory;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.type.TypeRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import com.wrmsr.presto.flat.FlatConnectorFactory;
import com.wrmsr.presto.flat.FlatModule;
import com.wrmsr.presto.functions.CompressionFunctions;
import com.wrmsr.presto.functions.DefineStructForQueryFunction;
import com.wrmsr.presto.functions.DefineStructFunction;
import com.wrmsr.presto.functions.GrokFunctions;
import com.wrmsr.presto.functions.PropertiesFunction;
import com.wrmsr.presto.functions.PropertiesType;
import com.wrmsr.presto.functions.SerializeFunction;
import com.wrmsr.presto.functions.StructManager;
import com.wrmsr.presto.hardcoded.HardcodedConnectorFactory;
import com.wrmsr.presto.hardcoded.HardcodedMetadataPopulator;
import com.wrmsr.presto.hardcoded.HardcodedModule;
import com.wrmsr.presto.jdbc.ExtendedJdbcClient;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnector;
import com.wrmsr.presto.jdbc.ExtendedJdbcConnectorFactory;
import com.wrmsr.presto.jdbc.h2.H2ClientModule;
import com.wrmsr.presto.jdbc.mysql.ExtendedMySqlClientModule;
import com.wrmsr.presto.jdbc.postgresql.ExtendedPostgreSqlClientModule;
import com.wrmsr.presto.jdbc.redshift.RedshiftClientModule;
import com.wrmsr.presto.jdbc.sqlite.SqliteClientModule;
import com.wrmsr.presto.jdbc.temp.TempClientModule;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerConnectorFactory;
import com.wrmsr.presto.metaconnectors.partitioner.PartitionerModule;
import com.wrmsr.presto.server.ModuleProcessor;
import com.wrmsr.presto.server.ServerEvent;
import com.wrmsr.presto.util.Configs;
import com.wrmsr.presto.util.Serialization;
import io.airlift.json.JsonCodec;
import org.apache.commons.configuration.HierarchicalConfiguration;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.YAML_OBJECT_MAPPER;

// import com.facebook.presto.type.ParametricType;

public class MainPlugin
        implements Plugin, ServerEvent.Listener
{
    private Map<String, String> optionalConfig = ImmutableMap.of();
    private ConnectorManager connectorManager;
    private TypeRegistry typeRegistry;
    private NodeManager nodeManager;
    private PluginManager pluginManager;
    private JsonCodec<ViewDefinition> viewCodec;
    private Metadata metadata;
    private SqlParser sqlParser;
    private List<PlanOptimizer> planOptimizers;
    private FeaturesConfig featuresConfig;
    private AccessControl accessControl;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null"));
    }

    @Inject
    public void setConnectorManager(ConnectorManager connectorManager)
    {
        this.connectorManager = checkNotNull(connectorManager);
    }

    @Inject
    public void setTypeRegistry(TypeRegistry typeRegistry)
    {
        this.typeRegistry = checkNotNull(typeRegistry);
    }

    @Inject
    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
    }

    @Inject
    public void setPluginManager(PluginManager pluginManager)
    {
        this.pluginManager = checkNotNull(pluginManager);
    }

    @Inject
    public void setViewCodec(JsonCodec<ViewDefinition> viewCodec)
    {
        this.viewCodec = viewCodec;
    }

    @Inject
    public void setMetadata(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Inject
    public void setSqlParser(SqlParser sqlParser)
    {
        this.sqlParser = sqlParser;
    }

    @Inject
    public void setPlanOptimizers(List<PlanOptimizer> planOptimizers)
    {
        this.planOptimizers = planOptimizers;
    }

    @Inject
    public void setFeaturesConfig(FeaturesConfig featuresConfig)
    {
        this.featuresConfig = featuresConfig;
    }

    @Inject
    public void setAccessControl(AccessControl accessControl)
    {
        this.accessControl = accessControl;
    }

    public static final Set<String> KNOWN_MODULE_NAMES = ImmutableSet.of(
            "aws",
            "hadoop"
    );

    public static class FileConfig
    {
        public final Map<String, String> jvm = ImmutableMap.of();
        public final Map<String, String> system = ImmutableMap.of();
        public final Map<String, String> log = ImmutableMap.of();
        public final List<String> plugins = ImmutableList.of();
        public final Map<String, Object> connectors = ImmutableMap.of();

        // FIXME gp plugin section
        public final Map<String, Object> clusters = ImmutableMap.of();
        public final Object aws = null;
    }

    public void installConfig(FileConfig fileConfig, StructManager structManager)
    {
        for (String plugin : fileConfig.plugins) {
            try {
                pluginManager.loadPlugin(plugin);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        for (Map.Entry<String, Object> e : fileConfig.connectors.entrySet()) {
            HierarchicalConfiguration hc = Configs.OBJECT_CONFIG_CODEC.encode(e.getValue());
            Map<String, String> connProps = newHashMap(Configs.CONFIG_PROPERTIES_CODEC.encode(hc));

            String targetConnectorName = connProps.get("connector.name");
            connProps.remove("connector.name");
            String targetName = e.getKey();
            connectorManager.createConnection(targetName, targetConnectorName, connProps);
            checkNotNull(connectorManager.getConnectors().get(targetName));
        }

        for (Connector connector : connectorManager.getConnectors().values()) {
            if (connector instanceof ExtendedJdbcConnector) {
                JdbcClient client = ((ExtendedJdbcConnector) connector).getJdbcClient();
                if (client instanceof ExtendedJdbcClient) {
                    ((ExtendedJdbcClient) client).runInitScripts();
                }
            }
        }
    }

    @Override
    public void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.ConnectorsLoaded) {
            StructManager structManager = new StructManager(
                    typeRegistry,
                    metadata
            );

            byte[] cfgBytes;
            try {
                cfgBytes = Files.readAllBytes(new File(System.getProperty("user.home") + "/presto/yelp-presto.yaml").toPath());
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            String cfgStr = new String(cfgBytes);
            for (Object part : Serialization.splitYaml(cfgStr)) {
                String partStr = Serialization.YAML.get().dump(part);

                ObjectMapper objectMapper = YAML_OBJECT_MAPPER.get();
                FileConfig fileConfig;
                try {
                    fileConfig = objectMapper.readValue(partStr.getBytes(), FileConfig.class);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                installConfig(fileConfig, structManager);
            }

            new HardcodedMetadataPopulator(
                    connectorManager,
                    viewCodec,
                    metadata,
                    sqlParser,
                    planOptimizers,
                    featuresConfig,
                    accessControl
            ).run();

            metadata.addFunctions(
                    new FunctionListBuilder(typeRegistry)
                            .scalar(CompressionFunctions.class)
                            .scalar(GrokFunctions.class)
                            .function(new SerializeFunction(metadata.getFunctionRegistry(), structManager))
                            .function(new DefineStructFunction(structManager))
                            .function(new DefineStructForQueryFunction(structManager, sqlParser, planOptimizers, featuresConfig, metadata, accessControl))
                            .function(new PropertiesFunction(typeRegistry))
                                    //.function(new ConnectFunction())
                            // .function(Hash.HASH)
                            .getFunctions());
        }
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ModuleProcessor.class) {
            return ImmutableList.of(type.cast(new ModuleProcessor()
            {
                @Override
                public Module apply(Module module)
                {
                    // jaxrsBinder(binder).bind(ShutdownResource.class);
                    return module;
                }
            }));
        }
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(
                    type.cast(new PartitionerConnectorFactory(optionalConfig, new PartitionerModule(null), getClassLoader(), connectorManager)),

                    type.cast(new FlatConnectorFactory(optionalConfig, new FlatModule(), getClassLoader())),

                    type.cast(new HardcodedConnectorFactory(optionalConfig, new HardcodedModule(), getClassLoader())),

                    type.cast(new ExtendedJdbcConnectorFactory("extended-mysql", new ExtendedMySqlClientModule(), optionalConfig, ImmutableMap.of(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("extended-postgresql", new ExtendedPostgreSqlClientModule(), optionalConfig, ImmutableMap.of(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("redshift", new RedshiftClientModule(), optionalConfig, RedshiftClientModule.createProperties(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("h2", new H2ClientModule(), optionalConfig, ImmutableMap.of(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("sqlite", new SqliteClientModule(), optionalConfig, ImmutableMap.of(), getClassLoader())),
                    type.cast(new ExtendedJdbcConnectorFactory("temp", new TempClientModule(), optionalConfig, TempClientModule.createProperties(), getClassLoader()))
            );
        }
        else if (type == Type.class) {
            return ImmutableList.of(
                    type.cast(PropertiesType.PROPERTIES)
            );
        }
        /*
        else if (type == com.facebook.presto.metadata.FunctionFactory.class) {
            return ImmutableList.of(
                    type.cast(new ExtensionFunctionFactory(typeRegistry, metadata.getFunctionRegistry()))
            );
        }
        */
        else if (type == ServerEvent.Listener.class) {
            return ImmutableList.of(type.cast(this));
        }
        return ImmutableList.of();
    }

    private static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), MainPlugin.class.getClassLoader());
    }
}
