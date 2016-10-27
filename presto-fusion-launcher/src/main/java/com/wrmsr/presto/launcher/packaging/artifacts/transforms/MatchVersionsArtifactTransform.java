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
package com.wrmsr.presto.launcher.packaging.artifacts.transforms;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.artifact.ArtifactType;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

@Immutable
public final class MatchVersionsArtifactTransform
        implements ArtifactTransform
{
    public static final Function<List<Artifact>, String> MAX_BY_STRING_VERSION =
            artifacts -> artifacts.stream().collect(Collectors.maxBy(comparing(Artifact::getVersion))).get().getVersion();

    private final Predicate<Artifact> predicate;
    private final Function<List<Artifact>, String> versionFunciton;

    public MatchVersionsArtifactTransform(Predicate<Artifact> predicate, Function<List<Artifact>, String> versionFunciton)
    {
        this.predicate = requireNonNull(predicate);
        this.versionFunciton = requireNonNull(versionFunciton);
    }

    @Override
    public List<Artifact> apply(ArtifactResolver artifactResolver, List<Artifact> artifacts)
    {
        Map<Boolean, List<Artifact>> groups = artifacts.stream().collect(Collectors.partitioningBy(predicate));
        artifacts = new ArrayList<>(groups.getOrDefault(false, ImmutableList.of()));

        if (groups.containsKey(true)) {
            List<Artifact> matchingArtifacts = groups.get(true);
            if (!matchingArtifacts.isEmpty()) {
                String version = versionFunciton.apply(matchingArtifacts);
                List<Artifact> newArtifacts = matchingArtifacts.stream()
                        .map(artifact -> new DefaultArtifact(
                                artifact.getGroupId(),
                                artifact.getArtifactId(),
                                artifact.getClassifier(),
                                artifact.getExtension(),
                                version,
                                artifact.getProperties(),
                                (ArtifactType) null))
                        .collect(toImmutableList());
                List<Artifact> resolvedNewArtifacts = artifactResolver.resolveArtifacts(newArtifacts);
                artifacts.addAll(resolvedNewArtifacts);
            }
        }

        return artifacts;
    }
}
