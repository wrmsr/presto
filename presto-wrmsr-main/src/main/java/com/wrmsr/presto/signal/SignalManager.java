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

import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.ServerConfig;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class SignalManager
{
    private final SignalConfig signalConfig;
    private final SignalHandling signalHandling;
    private final GracefulShutdownHandler gracefulShutdownHandler;
    private final boolean isCoordinator;

    @Inject
    public SignalManager(
            GracefulShutdownHandler gracefulShutdownHandler,
            SignalConfig signalConfig,
            SignalHandling signalHandling,
            ServerConfig serverConfig)
    {
        this.gracefulShutdownHandler = requireNonNull(gracefulShutdownHandler);
        this.signalConfig = requireNonNull(signalConfig);
        this.signalHandling = requireNonNull(signalHandling);
        this.isCoordinator = requireNonNull(serverConfig, "serverConfig is null").isCoordinator();
    }

    @PostConstruct
    private void installSignalHandlers()
    {
        // FIXME
    }
}
