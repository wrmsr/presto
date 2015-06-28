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
package com.wrmsr.presto.ruby;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.ScriptEngineProvider;
import com.google.common.collect.ImmutableList;
import org.jruby.embed.jsr223.JRubyEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RubyPlugin implements Plugin
{
    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig)
    {
    }

    @Override
    public <T> List<T> getServices(Class<T> type)
    {
        if (type == ScriptEngineProvider.class) {
            return ImmutableList.<T>of(
                    type.cast(
            new ScriptEngineProvider() {
                @Override
                public String getName()
                {
                    return "ruby";
                }

                @Override
                public ScriptEngine getScriptEngine()
                {
                    JRubyEngineFactory factory = new JRubyEngineFactory();
                    return factory.getScriptEngine();
                }
            }));
        }
        else {
            return ImmutableList.of();
        }
    }
}
