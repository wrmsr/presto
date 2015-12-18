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
package com.wrmsr.presto.hadoop.hive;

import com.google.common.base.Throwables;

public class HiveMetastore
{
    public void start()
    {
        new Thread()
        {
            @Override
            public void run()
            {
                try {
                    org.apache.hadoop.hive.metastore.HiveMetaStore.main(new String[] {});
                }
                catch (Throwable e) {
                    throw Throwables.propagate(e);
                }
            }
        }.start();
    }
}
