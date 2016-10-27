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
import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.AirliftArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.ArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.CachingArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.ArtifactTransform;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.MatchVersionsArtifactTransform;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.DefaultModelReader;
import org.apache.maven.model.io.ModelReader;
import org.sonatype.aether.artifact.Artifact;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.MoreCollectors.toArrayList;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class Packager
{
    private final ArtifactResolver artifactResolver;
    private final List<ArtifactTransform> artifactTransforms;

    @Immutable
    private static final class Module
    {
        private final Model model;
        private final Set<String> classPath;

        public Module(Model model, Set<String> classPath)
        {
            this.model = model;
            this.classPath = classPath;
        }
    }

    private final Map<String, Module> modules = new HashMap<>();
    private Module mainModule;

    public Packager(
            ArtifactResolver artifactResolver,
            List<ArtifactTransform> artifactTransforms)
    {
        this.artifactResolver = requireNonNull(artifactResolver);
        this.artifactTransforms = ImmutableList.copyOf(artifactTransforms);
    }

    public void addMainModule(File pomFile)
            throws IOException
    {
        checkState(mainModule == null);
        mainModule = addModuleInternal(pomFile);
    }

    public void addModule(File pomFile)
            throws IOException
    {
        addModuleInternal(pomFile);
    }

    public static Model readModel(File pomFile)
            throws IOException
    {
        checkArgument(pomFile.isFile());
        return new DefaultModelReader().read(pomFile, ImmutableMap.of(ModelReader.IS_STRICT, true));
    }

    public static Model readModule(Model model, String name)
            throws IOException
    {
        checkArgument(model.getModules().stream().anyMatch(name::equals));
        return readModel(new File(model.getProjectDirectory(), name));
    }

    public static Map<String, Model> readModules(Model model)
            throws IOException
    {
        Map<String, Model> modules = new LinkedHashMap<>();
        for (String module : model.getModules()) {
            checkState(!modules.containsKey(module));
            modules.put(module, readModule(model, module));
        }
        return modules;
    }

    private Module addModuleInternal(File pomFile)
            throws IOException
    {
        return addModuleInternal(readModel(pomFile));
    }

    private Module addModuleInternal(Model model)
            throws IOException
    {
        List<Artifact> artifacts = artifactResolver.resolvePom(model.getPomFile()).stream().collect(toArrayList());
        for (ArtifactTransform artifactTransform : artifactTransforms) {
            artifacts = artifactTransform.apply(artifactResolver, artifacts);
        }

        List<String> artifactStrings = artifacts.stream()
                .map(artifact -> String.format("%s-%s", artifact.getGroupId(), artifact.getArtifactId()))
                .collect(toImmutableList());
        checkState(new HashSet<>(artifactStrings).size() == artifactStrings.size());

        throw new IllegalArgumentException();
    }

    public static void main(String[] args)
            throws Exception
    {
        File parentPomFile = new File(System.getProperty("user.home") + "/src/wrmsr/presto/pom.xml");
        Model parentModel = readModel(parentPomFile);

        ArtifactResolver resolver = new CachingArtifactResolver(
                new AirliftArtifactResolver());

        List<ArtifactTransform> artifactTransforms = ImmutableList.of(
                new MatchVersionsArtifactTransform(
                        artifact -> "org.slf4j".equals(artifact.getGroupId()),
                        MatchVersionsArtifactTransform.MAX_BY_STRING_VERSION));

        Packager p = new Packager(resolver, artifactTransforms);

        p.addMainModule(new File(System.getProperty("user.home") + "/src/wrmsr/presto/presto-fusion-launcher/pom.xml"));
    }
}
