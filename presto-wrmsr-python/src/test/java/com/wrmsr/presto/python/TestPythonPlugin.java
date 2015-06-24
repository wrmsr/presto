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
package com.wrmsr.presto.python;

import org.python.core.Py;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import org.testng.annotations.Test;
import org.python.jsr223.PyScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class TestPythonPlugin
{
    @Test
    public void testStuff() throws Throwable
    {
        PythonInterpreter interp = PythonInterpreter.threadLocalStateInterpreter(null); // new PythonInterpreter();

        System.out.println("Hello, brave new world");
        interp.exec("import sys");
        interp.exec("print sys");

        interp.set("a", new PyInteger(42));
        interp.exec("print a");
        interp.exec("x = 2+2");
        PyObject x = interp.get("x");

        System.out.println("x: "+x);
        System.out.println("Goodbye, cruel world");
    }

    @Test
    public void testJsr223() throws Throwable
    {
        PyScriptEngineFactory factory = new PyScriptEngineFactory();

        PySystemState engineSys = new PySystemState();
        engineSys.path.append(Py.newString("my/lib/directory"));
        Py.setSystemState(engineSys);
        System.setProperty("python.home", "/Users/wtimoney/.m2/repository/org/python/jython/2.7.0/");

        ScriptEngine engine = factory.getScriptEngine();
        engine.eval("import sys");
        engine.eval("print sys");
        engine.put("a", 42);
        engine.eval("print a");
        engine.eval("x = 2 + 2");
        Object x = engine.get("x");
        System.out.println("x: " + x);
    }
}
