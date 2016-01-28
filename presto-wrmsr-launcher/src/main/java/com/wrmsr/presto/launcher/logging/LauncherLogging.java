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
package com.wrmsr.presto.launcher.logging;

import com.google.common.base.Throwables;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;
import io.airlift.log.SubprocessHandler;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

public class LauncherLogging
{
    public static void configure(LoggingConfig config)
    {
        Logging logging = Logging.initialize();
        try {
            logging.configure(new LoggingConfiguration());
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        for (Map.Entry<String, String> e : config.getLevels().entrySet()) {
            java.util.logging.Logger log = java.util.logging.Logger.getLogger(e.getKey());
            if (log != null) {
                Level level = Level.parse(e.getValue().toUpperCase());
                log.setLevel(level);
            }
        }

        for (LoggingConfig.AppenderConfig ac : config.getAppenders()) {
            if (ac instanceof LoggingConfig.SubprocessAppenderConfig) {
                LoggingConfig.SubprocessAppenderConfig sac = (LoggingConfig.SubprocessAppenderConfig) ac;
                java.util.logging.Logger.getLogger("").addHandler(new SubprocessHandler(sac.getArgs()));
            }
            else {
                throw new IllegalStateException();
            }
        }
    }
}
