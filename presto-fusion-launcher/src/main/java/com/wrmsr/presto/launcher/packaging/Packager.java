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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactCoordinate;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactName;
import com.wrmsr.presto.launcher.packaging.artifacts.Artifacts;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.AirliftArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.ArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.resolvers.CachingArtifactResolver;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.ArtifactTransform;
import com.wrmsr.presto.launcher.packaging.artifacts.transforms.MatchVersionsArtifactTransform;
import com.wrmsr.presto.launcher.packaging.jarBuilder.JarBuilder;
import com.wrmsr.presto.launcher.packaging.jarBuilder.JarBuilderEntries;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.FileJarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.jarBuilder.entries.JarBuilderEntry;
import com.wrmsr.presto.launcher.packaging.modules.PackagerModule;
import com.wrmsr.presto.launcher.packaging.modules.transforms.PackagerModuleTransform;
import com.wrmsr.presto.launcher.packaging.modules.transforms.ProjectModulePackagerModuleTransform;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import org.apache.maven.model.Model;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.MoreIO.deleteDirectory;
import static com.wrmsr.presto.util.collect.MoreCollectors.toArrayList;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableList;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableSet;
import static com.wrmsr.presto.util.collect.MoreCollectors.toOnly;
import static com.wrmsr.presto.util.collect.MoreOptionals.firstOrSameOptional;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public final class Packager
{
    private static final Logger log = Logger.get(Packager.class);

    private final ArtifactResolver artifactResolver;
    private final List<ArtifactTransform> artifactTransforms;
    private final List<PackagerModuleTransform> packagerModuleTransforms;

    private final Map<String, PackagerModule> modulesByName = new LinkedHashMap<>();

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

    public void addModule(Model model)
            throws IOException
    {
        addModuleInternal(model);
    }

    private PackagerModule addModuleInternal(Model model)
            throws IOException
    {
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
                        Optional.ofNullable(artifact.getFile()),
                        Optional.empty()))
                .collect(toImmutableList());
        for (PackagerModuleTransform packagerModuleTransform : packagerModuleTransforms) {
            packagerModules = packagerModuleTransform.apply(this, packagerModules);
        }

        ArtifactCoordinate moduleArtifactCoordinate = Models.getModelArtifactCoordinate(model);
        PackagerModule modulePackagerModule = packagerModules.stream()
                .filter(packagerModule -> moduleArtifactCoordinate.getName().equals(packagerModule.getArtifactCoordinate().getName()))
                .collect(toOnly());

        checkState(!modulePackagerModule.getClassPath().isPresent());
        Set<String> moduleClassPath = packagerModules.stream()
                .filter(packagerModule -> !moduleArtifactCoordinate.getName().equals(packagerModule.getArtifactCoordinate().getName()))
                .map(PackagerModule::getName)
                .collect(toImmutableSet());
        modulePackagerModule = new PackagerModule(
                modulePackagerModule.getArtifactCoordinate(),
                modulePackagerModule.getJarFile(),
                Optional.of(moduleClassPath));

        checkState(modulePackagerModule.getJarFile().isPresent());
        modulesByName.put(modulePackagerModule.getName(), modulePackagerModule);

        for (PackagerModule packagerModule : packagerModules) {
            checkState(packagerModule.getJarFile().isPresent());
            if (!packagerModule.getName().equals(modulePackagerModule.getName())) {
                if (modulesByName.containsKey(packagerModule.getName())) {
                    PackagerModule existingPackagerModule = modulesByName.get(packagerModule.getName());
                    checkState(packagerModule.getArtifactCoordinate().equals(existingPackagerModule.getArtifactCoordinate()));
                    modulesByName.put(
                            packagerModule.getName(),
                            new PackagerModule(
                                    packagerModule.getArtifactCoordinate(),
                                    firstOrSameOptional(packagerModule.getJarFile(), existingPackagerModule.getJarFile()),
                                    firstOrSameOptional(packagerModule.getClassPath(), existingPackagerModule.getClassPath())));
                }
                else {
                    modulesByName.put(packagerModule.getName(), packagerModule);
                }
            }
        }

        return modulePackagerModule;
    }

    private void buildJar(Model mainModel, File jarFile)
            throws IOException
    {
        checkState(jarFile.getParentFile().isDirectory());

        Map<String, JarBuilderEntry> jarBuilderEntries = new LinkedHashMap<>();

        File tmpDir = Files.createTempDir();
        tmpDir.deleteOnExit();

        try {
            File tmpContentsDir = new File(tmpDir, "contents");
            checkState(tmpContentsDir.mkdir());

            PackagerModule mainPackagerModule = null;
            for (PackagerModule packagerModule : modulesByName.values()) {
                if (!packagerModule.getArtifactCoordinate().getName().equals(Models.getModelArtifactName(mainModel))) {
                    continue;
                }
                checkState(mainPackagerModule == null);
                mainPackagerModule = packagerModule;

                Map<String, JarBuilderEntry> moduleJarBuilderEntries = buildModuleJarBuilderEntries(packagerModule, tmpContentsDir, "");
                for (Map.Entry<String, JarBuilderEntry> entry : moduleJarBuilderEntries.entrySet()) {
                    jarBuilderEntries.put(entry.getKey(), entry.getValue());
                }
            }

            checkState(mainPackagerModule != null);

            for (PackagerModule packagerModule : modulesByName.values()) {
                if (packagerModule == mainPackagerModule) {
                    continue;
                }
                File moduleDir = new File(tmpContentsDir, packagerModule.getName());
                checkState(!moduleDir.exists());
                java.nio.file.Files.createDirectories(moduleDir.toPath());

                String prefix = Strings.isNullOrEmpty(packagerModule.getName()) ? "" : packagerModule.getName() + "/";
                Map<String, JarBuilderEntry> moduleJarBuilderEntries = buildModuleJarBuilderEntries(packagerModule, moduleDir, prefix);
                for (Map.Entry<String, JarBuilderEntry> entry : moduleJarBuilderEntries.entrySet()) {
                    checkState(!jarBuilderEntries.containsKey(entry.getKey()));
                    jarBuilderEntries.put(entry.getKey(), entry.getValue());
                }
            }

            File tmpJarFile = new File(tmpDir, "jar");
            JarBuilder.buildJar(jarBuilderEntries, tmpJarFile);
            Jars.makeExecutableJar(tmpJarFile, jarFile);
        }
        finally {
            try {
                deleteDirectory(tmpDir.toPath());
            }
            catch (IOException e) {
                log.warn("Failed to delete temp dir", e);
            }
        }
    }

    private Map<String, JarBuilderEntry> buildModuleJarBuilderEntries(PackagerModule packagerModule, File moduleDir, String namePrefix)
            throws IOException
    {
        Map<String, JarBuilderEntry> jarBuilderEntries = new LinkedHashMap<>();

        List<JarBuilderEntry> moduleJarBuilderEntries = JarBuilder.getEntriesAsFiles(
                packagerModule.getJarFile().get(),
                moduleDir);
        for (JarBuilderEntry jarBuilderEntry : moduleJarBuilderEntries) {
            JarBuilderEntry renamedJarBuilderEntry = JarBuilderEntries.renameJarBuilderEntry(
                    jarBuilderEntry,
                    namePrefix + jarBuilderEntry.getName());

            checkState(!jarBuilderEntries.containsKey(renamedJarBuilderEntry.getName()));
            jarBuilderEntries.put(renamedJarBuilderEntry.getName(), renamedJarBuilderEntry);
        }

        if (packagerModule.getClassPath().isPresent()) {
            String manifestJarBuilderEntryName = namePrefix + Manifests.MANIFEST_PATH;
            FileJarBuilderEntry manifestJarBuilderEntry = (FileJarBuilderEntry) jarBuilderEntries.get(manifestJarBuilderEntryName);
            if (manifestJarBuilderEntry == null) {
                File pomFile = new File(moduleDir, "/" + Manifests.MANIFEST_PATH);
                checkState(!pomFile.exists());
                manifestJarBuilderEntry = new FileJarBuilderEntry(
                        namePrefix + Manifests.MANIFEST_PATH,
                        pomFile);
            }

            List<String> classPath = new ArrayList<>(packagerModule.getClassPath().get());
            Collections.sort(classPath);
            String moduleClassPath = Joiner.on(":").join(classPath);

            Manifest manifest = Manifests.parseManifest(readAllBytes(manifestJarBuilderEntry.getFile().toPath()));
            manifest.getMainAttributes().put(new Attributes.Name(Manifests.MANIFEST_CLASS_PATH_KEY), moduleClassPath);
            Files.write(Manifests.renderManifest(manifest), manifestJarBuilderEntry.getFile());
        }

        return jarBuilderEntries;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

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

        File outputJarFile = new File(System.getProperty("user.home") + "/fusion/fusion");
        packager.buildJar(Models.readModelModule(parentModel, mainModuleName), outputJarFile);
    }
}
