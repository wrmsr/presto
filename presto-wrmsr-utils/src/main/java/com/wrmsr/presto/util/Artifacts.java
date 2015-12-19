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
package com.wrmsr.presto.util;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.resolver.DefaultArtifact;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Artifacts
{
    private Artifacts()
    {
    }

    public static List<Artifact> sortedArtifacts(List<Artifact> artifacts)
    {
        List<Artifact> list = Lists.newArrayList(artifacts);
        Collections.sort(list, Ordering.natural().nullsLast().onResultOf(Artifact::getFile));
        return list;
    }

    public static List<URL> resolveModuleClassloaderUrls(String name)
    {
        try {
            if (!Strings.isNullOrEmpty(Repositories.getRepositoryPath())) {
                return Repositories.resolveUrlsForModule(name);
            }
            else {
                ArtifactResolver resolver = new ArtifactResolver(
                        ArtifactResolver.USER_LOCAL_REPO,
                        ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));
                File pom = null;
                for (String prefix : ImmutableList.of("", "../")) {
                    File f = new File(prefix + name + "/pom.xml");
                    if (f.exists()) {
                        pom = f;
                        break;
                    }
                }
                if (pom == null) {
                    throw new IOException("pom not found");
                }

                List<Artifact> artifacts = resolver.resolvePom(pom);
                artifacts = artifacts.stream().map(a -> {
                    for (String prefix : ImmutableList.of("", "../")) {
                        File f = new File(prefix + a.getArtifactId());
                        if (new File(f, "pom.xml").exists()) {
                            return new DefaultArtifact(
                                    a.getGroupId(),
                                    a.getArtifactId(),
                                    a.getClassifier(),
                                    a.getExtension(),
                                    a.getVersion(),
                                    a.getProperties(),
                                    new File(f, "target/classes"));
                        }
                    }
                    return a;
                }).collect(Collectors.toList());

                List<URL> urls = new ArrayList<>();
                for (Artifact artifact : sortedArtifacts(artifacts)) {
                    if (artifact.getFile() == null) {
                        throw new RuntimeException("Could not resolve artifact: " + artifact);
                    }
                    File file = artifact.getFile().getCanonicalFile();
                    urls.add(file.toURI().toURL());
                }
                return urls;
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
