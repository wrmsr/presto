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
package com.wrmsr.presto.launcher.cluster;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClusterUtils
{
    // TODO: uh oh. guess its about time the launcher got guiced.

    public static List getNodeConfig(ClusterConfig clusterConfig, String nodeName)
    {
        if (clusterConfig instanceof SimpleClusterConfig) {
            SimpleClusterConfig.Node nodeConfig = ((SimpleClusterConfig) clusterConfig).getNodes().get(nodeName);
            checkNotNull(nodeConfig, "Node not found in cluster: " + nodeName);
            return nodeConfig.getConfig();
        }
        else {
            throw new IllegalArgumentException(Objects.toString(clusterConfig));
        }
    }
}
