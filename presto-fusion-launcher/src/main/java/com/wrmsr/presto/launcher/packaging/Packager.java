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
package com.wrmsr.presto.launcher.packaging;

import com.google.common.collect.ImmutableList;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactCoordinate;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactName;
import com.wrmsr.presto.launcher.packaging.artifacts.Artifacts;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.AirliftArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.ArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.CachingArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.ArtifactTransform;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.MatchVersionsArtifactTransform;
import com.wrmsr.presto.launcher.packaging.modules.transforms.PackagerModuleTransform;
import com.wrmsr.presto.launcher.packaging.modules.transforms.ProjectModulePackagerModuleTransform;
import com.wrmsr.presto.launcher.packaging.modules.PackagerModule;
import org.apache.maven.model.Model;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.MoreCollectors.toArrayList;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class Packager
{
    private final ArtifactResolver artifactResolver;
    private final List<ArtifactTransform> artifactTransforms;
    private final List<PackagerModuleTransform> packagerModuleTransforms;

    private final Map<String, PackagerModule> modules = new HashMap<>();

    public Packager(
            ArtifactResolver artifactResolver,
            List<ArtifactTransform> artifactTransforms,
            List<PackagerModuleTransform> packagerModuleTransforms)
    {
        this.artifactResolver = requireNonNull(artifactResolver);
        this.artifactTransforms = ImmutableList.copyOf(artifactTransforms);
        this.packagerModuleTransforms = ImmutableList.copyOf(packagerModuleTransforms);
    }

    public ArtifactResolver getArtifactResolver()
    {
        return artifactResolver;
    }

    public void addModule(File pomFile)
            throws IOException
    {
        addModuleInternal(pomFile);
    }

    public void addModule(Model model)
            throws IOException
    {
        addModuleInternal(model);
    }

    private PackagerModule addModuleInternal(File pomFile)
            throws IOException
    {
        return addModuleInternal(Models.readModel(pomFile));
    }

    private PackagerModule addModuleInternal(Model model)
            throws IOException
    {
        ArtifactCoordinate moduleArtifactCoordinate = Models.getModelArtifactCoordinate(model);

        List<Artifact> artifacts = artifactResolver.resolvePom(model.getPomFile()).stream().collect(toArrayList());
        for (ArtifactTransform artifactTransform : artifactTransforms) {
            artifacts = artifactTransform.apply(artifactResolver, artifacts);
        }

        List<ArtifactName> artifactNames = artifacts.stream()
                .map(Artifacts::getArtifactName)
                .collect(toImmutableList());
        checkState(new HashSet<>(artifactNames).size() == artifactNames.size());

        List<PackagerModule> packagerModules = artifacts.stream()
                .map(artifact -> new PackagerModule(
                        Artifacts.getArtifactCoordinate(artifact),
                        null,
                        Optional.empty()))
                .collect(toImmutableList());

        throw new IllegalArgumentException();
    }

    public static void main(String[] args)
            throws Exception
    {
        File parentPomFile = new File(System.getProperty("user.home") + "/src/wrmsr/presto/pom.xml");
        Model parentModel = Models.readModel(parentPomFile);

        ArtifactResolver resolver = new CachingArtifactResolver(
                new AirliftArtifactResolver());

        List<ArtifactTransform> artifactTransforms = ImmutableList.of(
                new MatchVersionsArtifactTransform(
                        artifact -> "org.slf4j".equals(artifact.getGroupId()),
                        MatchVersionsArtifactTransform.MAX_BY_STRING_VERSION));

        List<PackagerModuleTransform> packagerModuleTransforms = ImmutableList.of(
                new ProjectModulePackagerModuleTransform(parentModel));

        Packager packager = new Packager(
                resolver,
                artifactTransforms,
                packagerModuleTransforms);

        String mainModuleName = "presto-fusion-launcher";
        List<String> moduleNames = ImmutableList.of(
                mainModuleName,

                "presto-blackhole",
                "presto-cassandra",
                "presto-cli",
                "presto-example-http",
                "presto-hive-hadoop2",
                "presto-jmx",
                "presto-kafka",
                "presto-local-file",
                "presto-main",
                "presto-ml",
                "presto-mysql",
                "presto-postgresql",
                "presto-raptor",
                "presto-redis",
                "presto-teradata-functions",
                "presto-tpch"
        );

        for (String moduleName : moduleNames) {
            packager.addModule(Models.readModelModule(parentModel, moduleName));
        }
    }
}
