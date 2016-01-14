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

import com.google.common.collect.MapMaker;
import com.wrmsr.presto.spi.scripting.ScriptEngineProvider;
import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;
import javax.script.ScriptEngine;

import java.util.Map;

public class ScriptingManager
{
    private final ScriptingConfig config;

    private volatile Map<String, ScriptEngineProvider> scriptEngineProviders = new MapMaker().makeMap();
    private volatile Map<String, Scripting> scriptings = new MapMaker().makeMap();

    // FIXME do like ConnectorSupportManager
    @Inject
    public ScriptingManager(
            ScriptingConfig config,
            Map<String, ScriptEngineProvider> scriptEngineProviders)
    {
        this.config = config;
        this.scriptEngineProviders.putAll(scriptEngineProviders);
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

    public void addScriptEngineProvider(ScriptEngineProvider p)
    {
        scriptEngineProviders.put(p.getName(), p);
    }

    public Scripting getScripting(String name)
    {
        return scriptings.get(name);
    }
}
