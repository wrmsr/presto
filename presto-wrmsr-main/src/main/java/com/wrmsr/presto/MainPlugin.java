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

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.metadata.ViewDefinition;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
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
import com.wrmsr.presto.config.ConfigContainer;
import com.wrmsr.presto.config.ConnectorsConfig;
import com.wrmsr.presto.config.MetaconnectorsConfig;
import com.wrmsr.presto.config.PluginsConfig;
import com.wrmsr.presto.config.PrestoConfig;
import com.wrmsr.presto.connector.MetaconnectorManager;
import com.wrmsr.presto.function.FunctionRegistration;
import com.wrmsr.presto.scripting.ScriptingManager;
import com.wrmsr.presto.server.ModuleProcessor;
import com.wrmsr.presto.server.ServerEventManager;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.util.GuiceUtils;
import com.wrmsr.presto.util.config.Configs;
import com.wrmsr.presto.util.config.PrestoConfigs;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.wrmsr.presto.util.Serialization.OBJECT_MAPPER;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class MainPlugin
        implements Plugin, ServerEvent.Listener
{
    private static final Logger log = Logger.get(MainPlugin.class);

    private final ConfigContainer config;
    private final Object lock = new Object();
    private volatile Injector injector;

    private MainOptionalConfig optionalConfig;

    private Injector mainInjector;
    private ConnectorManager connectorManager;
    private TypeRegistry typeRegistry;
    private PluginManager pluginManager;
    private Metadata metadata;
    private ServerEventManager serverEventManager;

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
    public void setMainInjector(Injector mainInjector)
    {
        this.mainInjector = mainInjector;
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
    public void setPluginManager(PluginManager pluginManager)
    {
        this.pluginManager = checkNotNull(pluginManager);
    }

    @Inject
    public void setMetadata(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Inject
    public void setServerEventManager(ServerEventManager serverEventManager)
    {
        this.serverEventManager = serverEventManager;
    }

    public static final List<Key> FORWARDED_INJECTIONS = ImmutableList.<Key>of(
            Key.get(NodeManager.class),
            Key.get(new TypeLiteral<JsonCodec<ViewDefinition>>() {}),
            Key.get(SqlParser.class),
            Key.get(new TypeLiteral<List<PlanOptimizer>>() {}),
            Key.get(FeaturesConfig.class),
            Key.get(AccessControl.class),
            Key.get(BlockEncodingSerde.class),
            Key.get(QueryManager.class),
            Key.get(SessionPropertyManager.class),
            Key.get(QueryIdGenerator.class)
    );

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
                binder.bind(TypeManager.class).toInstance(checkNotNull(typeRegistry));
                binder.bind(PluginManager.class).toInstance(checkNotNull(pluginManager));
                binder.bind(Metadata.class).toInstance(checkNotNull(metadata));
                binder.bind(ServerEventManager.class).toInstance(checkNotNull(serverEventManager));

                binder.bind(MainInjector.class).toInstance(new MainInjector(mainInjector));
                for (Key key : FORWARDED_INJECTIONS) {
                    binder.bind(key).toInstance(mainInjector.getInstance(key));
                }
            }
        };
    }

    private Module buildModule()
    {
        return Modules.combine(new MainPluginModule(config), buildInjectedModule());
    }

    private Injector buildInjector()
    {
        // TODO child injector?
        Bootstrap app = new Bootstrap(buildModule());

        try {
            return app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .initialize();
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
        if (event instanceof ServerEvent.MainPluginsLoaded) {
            final Injector injector = getInjector();

            Set<ServerEvent.Listener> sels = injector.getInstance(Key.get(new TypeLiteral<Set<ServerEvent.Listener>>() {}));
            for (ServerEvent.Listener sel : sels) {
                serverEventManager.addListener(sel);
            }

            for (String plugin : config.getMergedNode(PluginsConfig.class)) {
                try {
                    pluginManager.loadPlugin(plugin);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

            Set<Type> ts = injector.getInstance(Key.get(new TypeLiteral<Set<Type>>() {}));
            for (Type t : ts) {
                typeRegistry.addType(t);
            }

            Set<ParametricType> pts = injector.getInstance(Key.get(new TypeLiteral<Set<ParametricType>>() {}));
            for (ParametricType pt : pts) {
                typeRegistry.addParametricType(pt);
            }

            Set<FunctionRegistration> frs = injector.getInstance(Key.get(new TypeLiteral<Set<FunctionRegistration>>() {}));
            for (FunctionRegistration fr : frs) {
                metadata.addFunctions(fr.getFunctions(typeRegistry));
            }

            Set<SqlFunction> sfs = injector.getInstance(Key.get(new TypeLiteral<Set<SqlFunction>>() {}));
            for (SqlFunction sf : sfs) {
                metadata.addFunctions(new FunctionListBuilder(typeRegistry).function(sf).getFunctions());
            }

            Set<ConnectorFactory> cfs = injector.getInstance(Key.get(new TypeLiteral<Set<ConnectorFactory>>() {}));
            for (ConnectorFactory cf : cfs) {
                connectorManager.addConnectorFactory(cf);
            }

            Set<com.facebook.presto.spi.ConnectorFactory> lcfs = injector.getInstance(Key.get(new TypeLiteral<Set<com.facebook.presto.spi.ConnectorFactory>>() {}));
            for (com.facebook.presto.spi.ConnectorFactory cf : lcfs) {
                connectorManager.addConnectorFactory(cf);
            }

            MetaconnectorManager mm = injector.getInstance(MetaconnectorManager.class);
            for (Map.Entry<String, MetaconnectorsConfig.Entry> e : config.getMergedNode(MetaconnectorsConfig.class)) {
                mm.addMetaconnector(e.getKey(), e.getValue().getEntries());
            }

            // FIXME ugh
            ScriptingManager scriptingManager = injector.getInstance(ScriptingManager.class);
            scriptingManager.addConfigScriptings();
        }

        else if (event instanceof ServerEvent.MainConnectorsLoaded) {
            for (Map.Entry<String, ConnectorsConfig.Entry> e : config.getMergedNode(ConnectorsConfig.class).getEntries().entrySet()) {
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
                log.info(String.format("Loading connector: %s", targetConnectorName));
                connectorManager.createConnection(targetName, targetConnectorName, connProps);
                checkNotNull(connectorManager.getConnectors().get(targetName));
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
