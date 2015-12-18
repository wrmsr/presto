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

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.type.ParametricType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;
import com.wrmsr.presto.config.Config;
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.config.ConnectorsConfig;
import com.wrmsr.presto.config.ExecConfig;
import com.wrmsr.presto.config.PluginsConfig;
import com.wrmsr.presto.config.PrestoConfig;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.server.ModuleProcessor;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.type.PropertiesFunction;
import com.wrmsr.presto.util.GuiceUtils;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class MainPlugin
        implements Plugin, ServerEvent.Listener
{
    private static final Logger log = Logger.get(MainPlugin.class);

    private final ConfigContainer config;
    private final Object lock = new Object();
    private volatile Injector injector;

    private MainOptionalConfig optionalConfig;
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
    private BlockEncodingSerde blockEncodingSerde;
    private QueryManager queryManager;
    private SessionPropertyManager sessionPropertyManager;
    private QueryIdGenerator queryIdGenerator;

    public MainPlugin()
    {
        config = PrestoConfigs.loadConfigFromProperties(ConfigContainer.class);
    }

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
        this.optionalConfig = new MainOptionalConfig(ImmutableMap.copyOf(checkNotNull(optionalConfig, "optionalConfig is null")));
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

    @Inject
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    @Inject
    public void setQueryManager(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @Inject
    public void setSessionPropertyManager(SessionPropertyManager sessionPropertyManager)
    {
        this.sessionPropertyManager = sessionPropertyManager;
    }

    @Inject
    public void setQueryIdGenerator(QueryIdGenerator queryIdGenerator)
    {
        this.queryIdGenerator = queryIdGenerator;
    }

    private Module buildInjectedModule()
    {
        return new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(MainOptionalConfig.class).toInstance(checkNotNull(optionalConfig));
                binder.bind(ConnectorManager.class).toInstance(checkNotNull(connectorManager));
                binder.bind(TypeRegistry.class).toInstance(checkNotNull(typeRegistry));
                binder.bind(NodeManager.class).toInstance(checkNotNull(nodeManager));
                binder.bind(PluginManager.class).toInstance(checkNotNull(pluginManager));
                binder.bind(new TypeLiteral<JsonCodec<ViewDefinition>>() {}).toInstance(checkNotNull(viewCodec));
                binder.bind(Metadata.class).toInstance(checkNotNull(metadata));
                binder.bind(SqlParser.class).toInstance(checkNotNull(sqlParser));
                binder.bind(new TypeLiteral<List<PlanOptimizer>>() {}).toInstance(checkNotNull(planOptimizers));
                binder.bind(FeaturesConfig.class).toInstance(checkNotNull(featuresConfig));
                binder.bind(AccessControl.class).toInstance(checkNotNull(accessControl));
                binder.bind(BlockEncodingSerde.class).toInstance(checkNotNull(blockEncodingSerde));
                binder.bind(QueryManager.class).toInstance(checkNotNull(queryManager));
                binder.bind(SessionPropertyManager.class).toInstance(checkNotNull(sessionPropertyManager));
                binder.bind(QueryIdGenerator.class).toInstance(checkNotNull(queryIdGenerator));
            }
        };
    }

    private Module buildModule()
    {
        return Modules.combine(new MainPluginModule(config), buildInjectedModule());
    }

    private Injector buildInjector()
    {
        Bootstrap app = new Bootstrap(buildModule());

        try {
            return app.strictConfig().initialize();
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
            throw new IllegalStateException();
        }
    }

    private Injector getInjector()
    {
        if (injector == null) {
            synchronized (lock) {
                if (injector == null) {
                    injector = buildInjector();
                    postInject();
                }
            }
        }
        return checkNotNull(injector);
    }

    @Override
    public void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.PluginsLoaded) {
            for (String plugin : config.getMergedNode(PluginsConfig.class)) {
                try {
                    pluginManager.loadPlugin(plugin);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

            Set<Type> ts = getInjector().getInstance(Key.get(new TypeLiteral<Set<Type>>() {}));
            for (Type t : ts) {
                typeRegistry.addType(t);
            }

            Set<ParametricType> pts = getInjector().getInstance(Key.get(new TypeLiteral<Set<ParametricType>>() {}));
            for (ParametricType pt : pts) {
                typeRegistry.addParametricType(pt);
            }

            Set<FunctionRegistration> frs = getInjector().getInstance(Key.get(new TypeLiteral<Set<FunctionRegistration>>() {}));
            for (FunctionRegistration fr : frs) {
                metadata.addFunctions(fr.getFunctions(typeRegistry));
            }

            Set<SqlFunction> sfs = getInjector().getInstance(Key.get(new TypeLiteral<Set<SqlFunction>>() {}));
            for (SqlFunction sf : sfs) {
                metadata.addFunctions(new FunctionListBuilder(typeRegistry).function(sf).getFunctions());
            }

            Set<ConnectorFactory> cfs = getInjector().getInstance(Key.get(new TypeLiteral<Set<ConnectorFactory>>() {}));
            for (ConnectorFactory cf : cfs) {
                connectorManager.addConnectorFactory(cf);
            }

            metadata.addFunctions(
                    new FunctionListBuilder(typeRegistry)
                            .function(new PropertiesFunction(typeRegistry))
                            .getFunctions());

            /*
            for (Plugin plugin : pluginManager.getLoadedPlugins()) {
                for (ScriptEngineProvider scriptEngineProvider : plugin.getServices(ScriptEngineProvider.class)) {
                    log.info("Registering server event listener %s", serverEventListener.getClass().getName());
                    serverEventListeners.add(serverEventListener);
                }
            }
            */
        }

        else if (event instanceof ServerEvent.ConnectorsLoaded) {
            for (Config node : config.getNodes()) {
                if (node instanceof ConnectorsConfig) {
                    for (Map.Entry<String, ConnectorsConfig.Entry> e : ((ConnectorsConfig) node).getEntries().entrySet()) {
                        Object rt;
                        try {
                            rt = OBJECT_MAPPER.get().readValue(OBJECT_MAPPER.get().writeValueAsString(e.getValue()), Map.class);
                        }
                        catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                        HierarchicalConfiguration hc = Configs.OBJECT_CONFIG_CODEC.encode(rt);
                        Map<String, String> connProps = newHashMap(Configs.CONFIG_PROPERTIES_CODEC.encode(hc));

                        String targetConnectorName = connProps.get("connector.name");
                        connProps.remove("connector.name");
                        String targetName = e.getKey();
                        connectorManager.createConnection(targetName, targetConnectorName, connProps);
                        checkNotNull(connectorManager.getConnectors().get(targetName));
                    }
                }
            }
        }

        else if (event instanceof ServerEvent.DataSourcesLoaded) {
            for (Config node : config.getNodes()) {
                if (node instanceof ExecConfig) {
                    for (ExecConfig.Subject subject : ((ExecConfig) node).getSubjects().getSubjects()) {
                        if (subject instanceof ExecConfig.SqlSubject) {
                            for (ExecConfig.Verb verb : ((ExecConfig.SqlSubject) subject).getVerbs().getVerbs()) {
                                ImmutableList.Builder<String> builder = ImmutableList.builder();
                                if (verb instanceof ExecConfig.StringVerb) {
                                    builder.add(((ExecConfig.StringVerb) verb).getStatement());
                                }
                                else if (verb instanceof ExecConfig.FileVerb) {
                                    File file = new File(((ExecConfig.FileVerb) verb).getPath());
                                    byte[] b = new byte[(int) file.length()];
                                    try (FileInputStream fis = new FileInputStream(file)) {
                                        fis.read(b);
                                    }
                                    catch (IOException e) {
                                        throw Throwables.propagate(e);
                                    }
                                    builder.add(new String(b));
                                }
                                else {
                                    throw new IllegalArgumentException();
                                }
                                for (String s : builder.build()) {
                                    QueryId queryId = queryIdGenerator.createNextQueryId();
                                    Session session = Session.builder(sessionPropertyManager)
                                            .setQueryId(queryId)
                                            .setIdentity(new Identity("system", Optional.<Principal>empty()))
                                            .build();
                                    QueryInfo qi = queryManager.createQuery(session, s);

                                    while (!qi.getState().isDone()) {
                                        try {
                                            queryManager.waitForStateChange(qi.getQueryId(), qi.getState(), new Duration(10, TimeUnit.MINUTES));
                                            qi = queryManager.getQueryInfo(qi.getQueryId());
                                        }
                                        catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            ;
                                        }
                                    }
                                }
                            }
                        }

                        else if (subject instanceof ExecConfig.ConnectorSubject) {

                        }
                        else if (subject instanceof ExecConfig.ScriptSubject) {

                        }
                        else {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }
    }

    private void postInject()
    {
    }

    private Module processModule(Module module)
    {
        return GuiceUtils.combine(ImmutableList.of(module, new Module()
        {
            @Override
            public void configure(Binder binder)
            {
                jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);

                // jaxrsBinder(binder).bind(ShutdownResource.class);
            }
        }));
    }

    public static String getPomVersion(InputStream pomIn)
    {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        }
        catch (ParserConfigurationException e) {
            throw Throwables.propagate(e);
        }

        Document doc;
        try {
            doc = dBuilder.parse(pomIn);
        }
        catch (SAXException | IOException e) {
            throw Throwables.propagate(e);
        }

        Node project = doc.getDocumentElement();
        project.normalize();
        NodeList projectChildren = project.getChildNodes();
        Optional<Node> parent = IntStream.range(0, projectChildren.getLength()).boxed()
                .map(projectChildren::item)
                .filter(n -> "parent".equals(n.getNodeName()))
                .findFirst();
        NodeList parentChildren = parent.get().getChildNodes();
        Optional<Node> version = IntStream.range(0, parentChildren.getLength()).boxed()
                .map(parentChildren::item)
                .filter(n -> "version".equals(n.getNodeName()))
                .findFirst();
        return version.get().getTextContent();
    }

    public static String deducePrestoVersion()
    {
        InputStream pomIn = MainPlugin.class.getClassLoader().getResourceAsStream("META-INF/maven/com.wrmsr.presto/presto-wrmsr-main/pom.xml");
        if (pomIn == null) {
            try {
                pomIn = new FileInputStream(new File("pom.xml"));
            }
            catch (FileNotFoundException e) {
                throw Throwables.propagate(e);
            }
        }

        try {
            return getPomVersion(pomIn);
        }
        finally {
            try {
                pomIn.close();
            }
            catch (IOException e) {
                log.error(e);
            }
        }
    }

    private void autoConfigure()
    {
        if (Strings.isNullOrEmpty(System.getProperty("node.environment"))) {
            System.setProperty("node.environment", "default");
        }
        if (Strings.isNullOrEmpty(System.getProperty("presto.version"))) {
            System.setProperty("presto.version", deducePrestoVersion());
        }

        if (Strings.isNullOrEmpty(System.getProperty("node.id"))) {
            PrestoConfig.AutoNodeId autoNodeId = config.getMergedNode(PrestoConfig.class).getAutoNodeId();
            if (autoNodeId != null) {
                String nodeId;
                if (autoNodeId instanceof PrestoConfig.TempAutoNodeId) {
                    nodeId = UUID.randomUUID().toString();
                }
                else if (autoNodeId instanceof PrestoConfig.FileAutoNodeId) {
                    File file = new File(((PrestoConfig.FileAutoNodeId) autoNodeId).getFile());
                    if (file.exists()) {
                        try {
                            nodeId = new String(Files.readAllBytes(file.toPath())).trim();
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                    else {
                        nodeId = UUID.randomUUID().toString();
                        try {
                            Files.write(file.toPath(), (nodeId + "\n").getBytes());
                        }
                        catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }
                else {
                    throw new IllegalArgumentException(autoNodeId.toString());
                }
                checkState(nodeId.matches("[A-Za-z0-9\\-_]+"));
                System.setProperty("node.id", nodeId);
            }
        }
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ModuleProcessor.class) {
            autoConfigure();
            return ImmutableList.of(type.cast(new ModuleProcessor()
            {
                @Override
                public Module apply(Module module)
                {
                    return processModule(module);
                }
            }));
        }

        else if (type == ServerEvent.Listener.class) {
            return ImmutableList.of(type.cast(this));
        }

        else {
            return ImmutableList.of();
        }
    }

    protected static ClassLoader getClassLoader()
    {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), MainPlugin.class.getClassLoader());
    }
}
