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

import com.wrmsr.presto.launcher.packaging.Packager;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.ArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.ArtifactTransform;
import org.apache.maven.model.Model;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class ProjectModuleArtifactTransform
        implements ArtifactTransform
{
    private final Model parentModel;

    private List<Model> modules;

    public ProjectModuleArtifactTransform(Model parentModel)
    {
        this.parentModel = requireNonNull(parentModel);
    }

    @Override
    public List<Artifact> apply(ArtifactResolver artifactResolver, List<Artifact> artifacts)
            throws IOException
    {
        if (modules == null) {
            modules = Packager.readModules(parentModel).values().stream().collect(toImmutableList());
        }
        List<Artifact> newArtifacts = new ArrayList<>();
        for (Artifact artifact : artifacts) {

        }
        throw new IllegalArgumentException();
//        return newArtifacts;
    }
}
