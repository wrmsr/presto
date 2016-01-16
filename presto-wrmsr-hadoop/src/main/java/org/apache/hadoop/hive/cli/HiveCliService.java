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
package org.apache.hadoop.hive.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.sql.SQLException;

public class HiveCliService
{
    // FIXME hilariously not thread/session safe

    public static abstract class CommandException
            extends RuntimeException
    {
        private final String cmd;

        public CommandException(String cmd)
        {
            this.cmd = cmd;
        }

        public String getCmd()
        {
            return cmd;
        }

        @Override
        public String toString()
        {
            return getClass().getName() + "{" +
                    "cmd='" + cmd + '\'' +
                    '}';
        }
    }

    public static final class RejectedCommandException
            extends CommandException
    {
        public RejectedCommandException(String cmd)
        {
            super(cmd);
        }
    }

    public static final class FailedCommandException
            extends CommandException
    {
        private final SQLException cause;

        public FailedCommandException(String cmd, SQLException cause)
        {
            super(cmd);
            this.cause = cause;
        }

        @Override
        public SQLException getCause()
        {
            return cause;
        }

        @Override
        public String toString()
        {
            return getClass().getName() + "{" +
                    "cause=" + cause +
                    '}';
        }
    }

    private class Driver
            extends CliDriver
    {
        private Configuration conf;

        @Override
        void setConf(Configuration conf)
        {
            super.setConf(conf);
            this.conf = conf;
        }

        public int processCmd(String cmd)
        {
            CliSessionState ss = (CliSessionState) SessionState.get();
            ss.setLastCommand(cmd);
            // Flush the print stream, so it doesn't include output from the last command
            ss.err.flush();
            String cmd_trimmed = cmd.trim();
            String[] tokens = tokenizeCmd(cmd_trimmed);

            if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {
                throw new RejectedCommandException(cmd);
            }
            else if (tokens[0].equalsIgnoreCase("source")) {
                throw new RejectedCommandException(cmd);
            }
            else if (cmd_trimmed.startsWith("!")) {
                throw new RejectedCommandException(cmd);
            }
            else { // local mode
                try {
                    CommandProcessor proc = CommandProcessorFactory.get(tokens, (HiveConf) conf);
                    return processLocalCmd(cmd, proc, ss);
                }
                catch (SQLException e) {
                    throw new FailedCommandException(cmd, e);
                }
            }
        }

        private String[] tokenizeCmd(String cmd)
        {
            return cmd.split("\\s+");
        }
    }
}
