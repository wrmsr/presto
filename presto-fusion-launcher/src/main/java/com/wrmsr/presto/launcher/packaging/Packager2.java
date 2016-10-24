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
    private final ArtifactResolver artifactResolver;
    private final List<ArtifactTransform> artifactTransforms;

    @FunctionalInterface
    public interface ArtifactTransform
    {
        List<Artifact> apply(Packager2 packager, List<Artifact> artifacts);
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
                            .map(a -> new DefaultArtifact(a.getGroupId(), a.getArtifactId(), a.getClassifier(), a.getExtension(), version, a.getProperties(), (ArtifactType) null))
                            .collect(toImmutableList());
                    List<Artifact> resolvedNewArtifacts = packager.artifactResolver.resolveArtifacts(newArtifacts);
                    artifacts.addAll(resolvedNewArtifacts);
                }
            }

            return artifacts;
        }
    }

    @Immutable
    private static final class Module
    {
        private final File pomFile;
        private final Model model;
        private final String name;
        private final Set<String> classPath;

        public Module(File pomFile, Model model, String name, Set<String> classPath)
        {
            this.pomFile = pomFile;
            this.model = model;
            this.name = name;
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

    private Module addModuleInternal(File pomFile)
            throws IOException
    {
        checkArgument(pomFile.isFile());

        Model model = new DefaultModelReader().read(pomFile, ImmutableMap.of(ModelReader.IS_STRICT, true));

        List<Artifact> artifacts = artifactResolver.resolvePom(pomFile).stream().collect(toArrayList());
        for (ArtifactTransform artifactTransform : artifactTransforms) {
            artifacts = artifactTransform.apply(this, artifacts);
        }

        throw new IllegalArgumentException();
    }

    public static void main(String[] args)
            throws Exception
    {
        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        List<ArtifactTransform> artifactTransforms = ImmutableList.of(
                new MatchVersionsArtifactTransform(
                        a -> "org.slf4j".equals(a.getGroupId()),
                        MatchVersionsArtifactTransform.MAX_BY_STRING_VERSION));

        Packager2 p = new Packager2(resolver, artifactTransforms);
        p.addMainModule(new File("/Users/spinlock/src/wrmsr/presto/presto-fusion-launcher/pom.xml"));
    }
}
