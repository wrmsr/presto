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
package com.wrmsr.presto.launcher.server;

import com.google.common.base.Objects;
import com.google.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.wrmsr.presto.util.collect.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

final class SystemPropertyJvmArgumentForwarder
        implements ServerJvmArgumentProvider
{
    private final Set<ServerSystemPropertyProvider> serverSystemPropertyProviders;

    @Inject
    public SystemPropertyJvmArgumentForwarder(Set<ServerSystemPropertyProvider> serverSystemPropertyProviders)
    {
        this.serverSystemPropertyProviders = serverSystemPropertyProviders;
    }

    @Override
    public List<String> getServerJvmArguments()
    {
        Map<String, String> tmp = new HashMap<>();
        for (ServerSystemPropertyProvider p : serverSystemPropertyProviders) {
            Map<String, String> ps = p.getServerProperties();
            for (Map.Entry<String, String> e : ps.entrySet()) {
                String k = requireNonNull(e.getKey());
                String v = requireNonNull(e.getValue());
                if (tmp.containsKey(k)) {
                    String ev = tmp.get(k);
                    if (!Objects.equal(k, v)) {
                        throw new IllegalArgumentException(String.format("Got conflicting values for key %s: %s != %s", k, v, ev));
                    }
                }
                else {
                    tmp.put(k, v);
                }
            }
        }
        return tmp.entrySet().stream().map(e -> String.format("-D%s=%s", e.getKey(), e.getValue())).collect(toImmutableList());
    }
}
