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
package com.wrmsr.presto.metaconnector.partitioner;

import com.facebook.presto.spi.Connector;

import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionerTarget
{
    private final Connector target;

    public PartitionerTarget(Connector target)
    {
        this.target = checkNotNull(target);
    }

    public Connector getTarget()
    {
        return target;
    }

    @Override
    public String toString()
    {
        return "PartitionerConnectorTarget{" +
                "target=" + target +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionerTarget that = (PartitionerTarget) o;

        return !(target != null ? !target.equals(that.target) : that.target != null);
    }

    @Override
    public int hashCode()
    {
        return target != null ? target.hashCode() : 0;
    }
}
