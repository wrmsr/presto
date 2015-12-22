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
package com.wrmsr.presto.hadoop;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public interface HadoopService
{
    enum State
    {
        READY,
        STARTED,
        STOPPED
    }

    void start();

    State getState();

    void stop();

    void join();

    abstract class ThreadImpl
            implements HadoopService
    {
        private volatile Thread thread;

        protected abstract Thread createThread();

        @Override
        public synchronized void start()
        {
            checkState(thread == null, "already started");

            Thread thread = createThread();
            thread.start();
            this.thread = thread;
        }

        @Override
        public synchronized State getState()
        {
            if (thread == null) {
                return State.READY;
            }
            else if (thread.isAlive()) {
                return State.STARTED;
            }
            else {
                return State.STOPPED;
            }
        }

        @Override
        public void stop()
        {
            checkState(thread != null, "not started");
            checkState(thread.isAlive(), "already stopped");

            thread.interrupt();
            try {
                thread.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public synchronized void join()
        {
            checkState(thread != null, "not started");

            try {
                thread.join();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    abstract class StandardImpl
            extends ThreadImpl
    {
        private final String configName;
        private final String className;

        private final Map<String, String> properties;
        private final String[] args;

        public StandardImpl(String configName, String className, Map<String, String> properties, String[] args)
        {
            this.configName = configName;
            this.className = className;
            this.properties = properties;
            this.args = args;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        protected Thread createThread()
        {
            return HadoopUtils.hadoopRunnableThread(() -> {
                HadoopUtils.installConfig(configName, properties);
                Class cls = Thread.currentThread().getContextClassLoader().loadClass(className);
                cls.getMethod("main", String[].class).invoke(null, new Object[] {args});
            });
        }
    }
}
