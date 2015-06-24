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

import org.jruby.Ruby;
import org.jruby.RubyRuntimeAdapter;
import org.jruby.javasupport.JavaEmbedUtils;
import org.testng.annotations.Test;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.List;

public class TestRubyPlugin
{
    @Test
    public void testStuff() throws Throwable
    {
        List<String> loadPaths = new ArrayList<String>();
        Ruby runtime = JavaEmbedUtils.initialize(loadPaths);
        RubyRuntimeAdapter evaler = JavaEmbedUtils.newRuntimeAdapter();

        evaler.eval(runtime, "puts 1+2");

        JavaEmbedUtils.terminate(runtime);
    }

    private Jsr223SimpleEvalSample() throws ScriptException {
        System.out.println("[" + getClass().getName() + "]");
        defaultBehavior();
        transientBehavior();
    }

    private void defaultBehavior() throws ScriptException {
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("jruby");
        Bindings bindings = new SimpleBindings();
        bindings.put("message", "global variable");
        String script =
                "puts $message";
        engine.eval(script, bindings);
    }

    private void transientBehavior() throws ScriptException
    {
        System.setProperty("org.jruby.embed.localvariable.behavior", "transient");
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("jruby");
        Bindings bindings = new SimpleBindings();
        bindings.put("message", "local variable");
        bindings.put("@message", "instance variable");
        bindings.put("$message", "global variable");
        bindings.put("MESSAGE", "constant");
        String script =
                "puts message\n" +
                        "puts @message\n" +
                        "puts $message\n" +
                        "puts MESSAGE";
        engine.eval(script, bindings);
    }
}
