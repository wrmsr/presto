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
package com.wrmsr.presto.functions;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import org.testng.annotations.Test;

public class TestGrokFunctions
{
    @Test
    public void testStuff() throws Throwable
    {
        Grok grok = Grok.create("grok-patterns/grok-patterns");
        grok.compile("%{COMBINEDAPACHELOG}");
        String log = "112.169.19.192 - - [06/Mar/2013:01:36:30 +0900] \"GET / HTTP/1.1\" 200 44346 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.22 (KHTML, like Gecko) Chrome/25.0.1364.152 Safari/537.22\"";
        Match gm = grok.match(log);
        gm.captures();
        System.out.println(gm.toJson());
    }
}
