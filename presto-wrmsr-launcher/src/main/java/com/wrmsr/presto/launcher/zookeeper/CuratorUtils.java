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
package com.wrmsr.presto.launcher.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.lang.reflect.Field;
import java.util.Map;

public class CuratorUtils
{
    public static void startClient(CuratorFramework client)
    {
        if (client.getState() == CuratorFrameworkState.STARTED) {
            return;
        }
        client.start();
        try {
            client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Failed to start CuratorFramework.", e);
        }
        if (client.getState() != CuratorFrameworkState.STARTED) {
            throw new RuntimeException("Failed to start CuratorFramework.");
        }
    }

    public static String getMutexThreadLockPath(InterProcessLock mutex, Thread thread)
    {
        try {
            Field threadDataField = InterProcessMutex.class.getDeclaredField("threadData");
            threadDataField.setAccessible(true);
            Map threadData = (Map) threadDataField.get(mutex);
            Object lockData = threadData.get(thread);
            if (lockData == null) {
                return null;
            }
            Field lockPathField = lockData.getClass().getDeclaredField("lockPath");
            lockPathField.setAccessible(true);
            String lockPath = (String) lockPathField.get(lockData);
            return lockPath;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Error getting curator mutex thread lock path", e);
        }
    }
}
