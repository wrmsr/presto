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
import com.facebook.presto.metadata.SqlFunction;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.wrmsr.presto.MainModule;
import com.wrmsr.presto.config.ConfigContainer;

import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class EvalModule
        extends MainModule
{

    @Override
    public Set<Key> getInjectorForwardings(ConfigContainer config)
    {
        return ImmutableSet.of(
                Key.get(ConnectorManager.class));
    }

    @Override
    public void configurePlugin(ConfigContainer config, Binder binder)
    {
        binder.bind(EvalManager.class).asEagerSingleton();

        binder.bind(ExecFunction.class).asEagerSingleton();
        newSetBinder(binder, SqlFunction.class).addBinding().to(ExecFunction.class);
    }

    /*
                for (Config node : config.getNodes()) {
                if (node instanceof ExecConfig) {
                    for (ExecConfig.Subject subject : ((ExecConfig) node).getSubjects().getSubjects()) {
                        if (subject instanceof ExecConfig.SqlSubject) {
                            for (ExecConfig.Verb verb : ((ExecConfig.SqlSubject) subject).getVerbs().getVerbs()) {
                                ImmutableList.Builder<String> builder = ImmutableList.builder();
                                if (verb instanceof ExecConfig.StringVerb) {
                                    builder.add(((ExecConfig.StringVerb) verb).getStatement());
                                }
                                else if (verb instanceof ExecConfig.FileVerb) {
                                    File file = new File(((ExecConfig.FileVerb) verb).getPath());
                                    byte[] b = new byte[(int) file.length()];
                                    try (FileInputStream fis = new FileInputStream(file)) {
                                        fis.read(b);
                                    }
                                    catch (IOException e) {
                                        throw Throwables.propagate(e);
                                    }
                                    builder.add(new String(b));
                                }
                                else {
                                    throw new IllegalArgumentException();
                                }
                                for (String s : builder.build()) {
                                    QueryId queryId = queryIdGenerator.createNextQueryId();
                                    Session session = Session.builder(sessionPropertyManager)
                                            .setQueryId(queryId)
                                            .setIdentity(new Identity("system", Optional.<Principal>empty()))
                                            .build();
                                    QueryInfo qi = queryManager.createQuery(session, s);

                                    while (!qi.getState().isDone()) {
                                        try {
                                            queryManager.waitForStateChange(qi.getQueryId(), qi.getState(), new Duration(10, TimeUnit.MINUTES));
                                            qi = queryManager.getQueryInfo(qi.getQueryId());
                                        }
                                        catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                            ;
                                        }
                                    }
                                }
                            }
                        }

                        else if (subject instanceof ExecConfig.ConnectorSubject) {

                        }
                        else if (subject instanceof ExecConfig.ScriptSubject) {

                        }
                        else {
                            throw new IllegalArgumentException();
                        }
                    }
                }
     */
}
