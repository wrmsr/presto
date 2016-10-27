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
package com.wrmsr.presto.launcher.packaging.artifacts.resolvers;

import com.google.common.collect.ImmutableList;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class AirliftArtifactResolver
        implements ArtifactResolver
{
    private final io.airlift.resolver.ArtifactResolver airliftArtifactResolver;

    public AirliftArtifactResolver(io.airlift.resolver.ArtifactResolver airliftArtifactResolver)
    {
        this.airliftArtifactResolver = requireNonNull(airliftArtifactResolver);
    }

    public AirliftArtifactResolver(String localRepositoryDir, List<String> remoteRepositoryUris)
    {
        this(new io.airlift.resolver.ArtifactResolver(localRepositoryDir, remoteRepositoryUris));
    }

    public AirliftArtifactResolver()
    {
        this(
                io.airlift.resolver.ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(io.airlift.resolver.ArtifactResolver.MAVEN_CENTRAL_URI));
    }

    @Override
    public List<Artifact> resolveArtifacts(Iterable<? extends Artifact> sourceArtifacts)
    {
        return airliftArtifactResolver.resolveArtifacts(sourceArtifacts);
    }

    @Override
    public List<Artifact> resolvePom(File pomFile)
    {
        return airliftArtifactResolver.resolvePom(pomFile);
    }
}
