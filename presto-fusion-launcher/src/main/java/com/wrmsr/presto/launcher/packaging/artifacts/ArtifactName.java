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
package com.wrmsr.presto.launcher.packaging.artifacts;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class ArtifactName
{
    private final String groupId;
    private final String artifactId;

    public ArtifactName(String groupId, String artifactId)
    {
        this.groupId = requireNonNull(groupId);
        this.artifactId = requireNonNull(artifactId);
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
        ArtifactName that = (ArtifactName) o;
        return Objects.equals(groupId, that.groupId) &&
                Objects.equals(artifactId, that.artifactId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(groupId, artifactId);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("groupId", groupId)
                .add("artifactId", artifactId)
                .toString();
    }

    public String getGroupId()
    {
        return groupId;
    }

    public String getArtifactId()
    {
        return artifactId;
    }
}
