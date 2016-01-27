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
package com.wrmsr.presto.signal;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class SunSignalHandling
        implements SignalHandling
{
    @Override
    public void registerSignalHandler(String name, Handler handler)
    {
        Signal.handle(new Signal(name), new SignalHandler()
        {
            @Override
            public void handle(Signal signal)
            {
                handler.accept(name);
            }
        });
    }
}
