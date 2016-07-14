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

import com.google.inject.Binder;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;

public class SignalModule
        extends MainModule
{
    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(SunSignalHandling_.class).asEagerSingleton();
        binder.bind(SignalHandling.class).to(SunSignalHandling_.class);

        binder.bind(SignalManager.class).asEagerSingleton();
    }
}
