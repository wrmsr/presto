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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.wrmsr.presto.launcher.runtime.Repositories;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.apache.maven.repository.internal.MavenRepositorySystemSession;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Artifacts
{
    private Artifacts()
    {
    }

    public static <T> Stream<T> toStream(Optional<T> opt)
    {
        if (opt.isPresent()) {
            return Stream.of(opt.get());
        }
        else {
            return Stream.empty();
        }
    }

    public static ArtifactName getArtifactName(Artifact artifact)
    {
        return new ArtifactName(
                artifact.getGroupId(),
                artifact.getArtifactId());
    }

    public static ArtifactCoordinate getArtifactCoordinate(Artifact artifact)
    {
        return new ArtifactCoordinate(
                getArtifactName(artifact),
                artifact.getVersion());
    }

    public static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    private static void silenceAirliftArtifactResolvedLogger(ArtifactResolver resolver)
    {
        try {
            Field field = resolver.getClass().getDeclaredField("repositorySystemSession");
            field.setAccessible(true);
            MavenRepositorySystemSession session = (MavenRepositorySystemSession) field.get(resolver);
            session.setRepositoryListener(null);
        }
        catch (ReflectiveOperationException e) {
            return;
        }
    }
}
