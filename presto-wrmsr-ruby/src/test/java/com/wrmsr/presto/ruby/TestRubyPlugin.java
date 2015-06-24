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
}
