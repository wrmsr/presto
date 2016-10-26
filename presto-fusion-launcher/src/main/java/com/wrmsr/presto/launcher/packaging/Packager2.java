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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.DefaultModelReader;
import org.apache.maven.model.io.ModelReader;
import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.artifact.ArtifactType;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.annotation.concurrent.Immutable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.MoreCollectors.toArrayList;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class Packager2
{
    public interface ArtifactResolver
    {
        List<Artifact> resolveArtifacts(Iterable<? extends Artifact> sourceArtifacts);

        List<Artifact> resolvePom(File pomFile);
    }

    public static final class CachingArtifactResolver
            implements ArtifactResolver
    {
        private final ArtifactResolver wrapped;

        private final Map<File, List<Artifact>> resolvePomCache = new HashMap<>();

        public CachingArtifactResolver(ArtifactResolver wrapped)
        {
            this.wrapped = requireNonNull(wrapped);
        }

        @Override
        public List<Artifact> resolveArtifacts(Iterable<? extends Artifact> sourceArtifacts)
        {
            return wrapped.resolveArtifacts(sourceArtifacts);
        }

        @Override
        public List<Artifact> resolvePom(File pomFile)
        {
            if (!resolvePomCache.containsKey(pomFile)) {
                List<Artifact> artifacts = ImmutableList.copyOf(wrapped.resolvePom(pomFile));
                resolvePomCache.put(pomFile, artifacts);
            }
            return requireNonNull(resolvePomCache.get(pomFile));
        }
    }

    public static final class AirliftArtifactResolver
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

    private final ArtifactResolver artifactResolver;
    private final List<ArtifactTransform> artifactTransforms;

    @FunctionalInterface
    public interface ArtifactTransform
    {
        List<Artifact> apply(Packager2 packager, List<Artifact> artifacts)
                throws IOException;
    }

    @Immutable
    public static final class IgnoreArtifactTransform
            implements ArtifactTransform
    {
        private final List<Predicate<Artifact>> ignorePredicates;

        public IgnoreArtifactTransform(List<Predicate<Artifact>> ignorePredicates)
        {
            this.ignorePredicates = ImmutableList.copyOf(ignorePredicates);
        }

        @Override
        public List<Artifact> apply(Packager2 packager, List<Artifact> artifacts)
        {
            return artifacts.stream()
                    .filter(artifact -> !ignorePredicates.stream().anyMatch(predicate -> predicate.test(artifact)))
                    .collect(toArrayList());
        }
    }

    @Immutable
    public static final class MatchVersionsArtifactTransform
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
        public List<Artifact> apply(Packager2 packager, List<Artifact> artifacts)
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
                    List<Artifact> resolvedNewArtifacts = packager.artifactResolver.resolveArtifacts(newArtifacts);
                    artifacts.addAll(resolvedNewArtifacts);
                }
            }

            return artifacts;
        }
    }

    @Immutable
    public static final class ProjectModuleArtifactTransform
            implements ArtifactTransform
    {
        private final Model parentModel;

        public ProjectModuleArtifactTransform(Model parentModel)
        {
            this.parentModel = requireNonNull(parentModel);
        }

        @Override
        public List<Artifact> apply(Packager2 packager, List<Artifact> artifacts)
        {
            throw new UnsupportedOperationException();
        }
    }

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

    public Packager2(
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

    private static Document readXml(File file)
            throws IOException
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setValidating(false);
        DocumentBuilder db;
        try {
            db = dbf.newDocumentBuilder();
        }
        catch (ParserConfigurationException e) {
            throw Throwables.propagate(e);
        }
        try {
            return db.parse(file);
        }
        catch (SAXException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Model readModel(File pomFile)
            throws IOException
    {
        checkArgument(pomFile.isFile());
        return new DefaultModelReader().read(pomFile, ImmutableMap.of(ModelReader.IS_STRICT, true));
    }

    public static Model getSubmodule(Model model, String name)
            throws IOException
    {
        checkArgument(model.getModules().stream().anyMatch(name::equals));
        return readModel(new File(model.getProjectDirectory(), name));
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
            artifacts = artifactTransform.apply(this, artifacts);
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

        Packager2 p = new Packager2(resolver, artifactTransforms);

        p.addMainModule(new File(System.getProperty("user.home") + "/src/wrmsr/presto/presto-fusion-launcher/pom.xml"));
    }
}
