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
package com.wrmsr.presto.launcher.packaging.modules;

import com.google.common.collect.ImmutableSet;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactCoordinate;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public final class PackagerModule
{
    private final ArtifactCoordinate artifactCoordinate;
    private final Optional<File> jarFile;
    private final Optional<Set<String>> classPath;

    private final String name;

    public PackagerModule(ArtifactCoordinate artifactCoordinate, Optional<File> jarFile, Optional<Set<String>> classPath)
    {
        this.artifactCoordinate = requireNonNull(artifactCoordinate);
        this.jarFile = requireNonNull(jarFile);
        this.classPath = requireNonNull(classPath).map(ImmutableSet::copyOf);

        this.name = getName(artifactCoordinate);
    }

    public ArtifactCoordinate getArtifactCoordinate()
    {
        return artifactCoordinate;
    }

    public Optional<File> getJarFile()
    {
        return jarFile;
    }

    public Optional<Set<String>> getClassPath()
    {
        return classPath;
    }

    public String getName()
    {
        return name;
    }

    public static void checkValidName(ArtifactCoordinate artifactCoordinate)
    {
        for (String part : new String[] {
                artifactCoordinate.getName().getGroupId(),
                artifactCoordinate.getName().getArtifactId(),
                artifactCoordinate.getVersion()}) {
            checkArgument(part.indexOf('/') < 0);
            checkArgument(part.indexOf(':') < 0);
            checkArgument(!part.contains("\\.\\."));
        }
    }

    public static String getName(ArtifactCoordinate artifactCoordinate)
    {
        checkValidName(artifactCoordinate);
        return String.format(
                "%s/%s/%s/%s-%s.jar",
                artifactCoordinate.getName().getGroupId().replaceAll("\\.", "/"),
                artifactCoordinate.getName().getArtifactId(),
                artifactCoordinate.getVersion(),
                artifactCoordinate.getName().getArtifactId(),
                artifactCoordinate.getVersion());
    }
}
