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
package com.wrmsr.presto.scripting;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.MapMaker;
import com.wrmsr.presto.spi.ServerEvent;
import com.wrmsr.presto.spi.scripting.ScriptEngineProvider;
import io.airlift.log.Logger;
import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;
import javax.script.ScriptEngine;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScriptingManager
    implements ServerEvent.Listener
{
    private static final Logger log = Logger.get(ScriptingManager.class);

    private final ScriptingConfig config;

    private final Map<String, ScriptEngineProvider> scriptEngineProviders = new MapMaker().makeMap();
    private final Map<String, Scripting> scriptings = new MapMaker().makeMap();

    // FIXME do like ConnectorSupportManager
    @Inject
    public ScriptingManager(
            ScriptingConfig config,
            Set<ScriptEngineProvider> scriptEngineProviders,
            List<Plugin> mainPlugins)
    {
        this.config = config;
        scriptEngineProviders.forEach(this::register);
        mainPlugins.forEach(this::register);
    }

    @Override
    public synchronized void onServerEvent(ServerEvent event)
    {
        if (event instanceof ServerEvent.PluginLoaded) {
            register(((ServerEvent.PluginLoaded) event).getPlugin());
        }
    }

    public synchronized void register(Plugin plugin)
    {
        for (ScriptEngineProvider provider : plugin.getServices(ScriptEngineProvider.class)) {
            register(provider);
        }
    }

    public synchronized void register(ScriptEngineProvider provider)
    {
        for (String name : provider.getNames()) {
            log.info("Registering script engine provider %s -> %s", name, provider);
            scriptEngineProviders.put(name, provider);
        }
    }

    public void addConfigScriptings()
    {
        for (Map.Entry<String, ScriptingConfig.Entry> e : config.getEntries().entrySet()) {
            addScripting(e.getKey(), e.getValue());
        }
    }

    public void addScripting(String name, ScriptingConfig.Entry entry)
    {
        ScriptEngine e = scriptEngineProviders.get(entry.getEngine()).getScriptEngine();
        scriptings.put(name, new Scripting(name, e));
    }

    public Scripting getScripting(String name)
    {
        return scriptings.get(name);
    }
}
