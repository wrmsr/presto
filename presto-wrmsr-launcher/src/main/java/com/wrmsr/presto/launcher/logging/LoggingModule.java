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

import com.wrmsr.presto.launcher.LauncherModule;
import com.wrmsr.presto.launcher.config.ConfigContainer;
import io.airlift.log.SubprocessHandler;

import java.util.Map;
import java.util.logging.Level;

public class LoggingModule
        extends LauncherModule
{
    @Override
    public void configureServerLogging(ConfigContainer config)
    {
        LoggingConfig loggingConfig = config.getMergedNode(LoggingConfig.class);

        // fixme fuck my life
//        for (Map.Entry<String, String> e : loggingConfig.getLevels().entrySet()) {
//            java.util.logging.Logger log = java.util.logging.Logger.getLogger(e.getKey());
//            if (log != null) {
//                Level level = Level.parse(e.getValue().toUpperCase());
//                log.setLevel(level);
//            }
//        }

        for (LoggingConfig.AppenderConfig ac : loggingConfig.getAppenders()) {
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
