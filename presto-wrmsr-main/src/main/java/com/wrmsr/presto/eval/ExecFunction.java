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
package com.wrmsr.presto.eval;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.Connector;
import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.connectorSupport.ConnectorSupportManager;
import com.wrmsr.presto.function.StringVarargsFunction;
import com.wrmsr.presto.spi.connectorSupport.EvalConnectorSupport;
import com.wrmsr.presto.util.jdbc.ScriptRunner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.inject.Inject;

import java.io.StringReader;
import java.lang.invoke.MethodHandle;

public class ExecFunction
        extends StringVarargsFunction
{
    private final ConnectorManager connectorManager;
    private final ConnectorSupportManager connectorSupportManager;

    @Inject
    public ExecFunction(ConnectorManager connectorManager, ConnectorSupportManager connectorSupportManager)
    {
        super(
                "connector_exec",
                "execute raw connector statements",
                ImmutableList.of("varchar"),
                1,
                "varchar",
                "connector_exec",
                ImmutableList.of(Context.class, ConnectorSession.class, Slice.class));
        this.connectorManager = connectorManager;
        this.connectorSupportManager = connectorSupportManager;
    }

    private static class Context
    {
        private final ConnectorManager connectorManager;
        private final ConnectorSupportManager connectorSupportManager;

        public Context(ConnectorManager connectorManager, ConnectorSupportManager connectorSupportManager)
        {
            this.connectorManager = connectorManager;
            this.connectorSupportManager = connectorSupportManager;
        }
    }

    @Override
    protected MethodHandle bindMethodHandle()
    {
        return super.bindMethodHandle().bindTo(new Context(connectorManager, connectorSupportManager));
    }

    public static Slice connector_exec(Context context, ConnectorSession connectorSession, Slice connectorName, Slice[] commands)
    {
        Connector c = context.connectorManager.getConnectors().get(connectorName.toStringUtf8());
        EvalConnectorSupport ecs = context.connectorSupportManager.getConnectorSupport(EvalConnectorSupport.class, connectorSession, c).get();
        for (int i = 0; i < commands.length; ++i) {
            String cmd = commands[i].toStringUtf8();
            ecs.exec(cmd);
        }
        return Slices.EMPTY_SLICE;
    }
}
