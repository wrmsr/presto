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
package com.wrmsr.presto.wrapper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logging;
import io.airlift.resolver.ArtifactResolver;
import io.airlift.log.Logger;
import org.sonatype.aether.artifact.Artifact;

import java.io.File;
import java.net.URL;
import java.util.List;

public class PrestoWrapperBuilder
{
    private static final Logger log = Logger.get(PrestoWrapperBuilder.class);

    private PrestoWrapperBuilder()
    {
    }

    public static void main(String[] args)
            throws Throwable
    {
        new PrestoWrapperBuilder().run(args);
    }

    public void run(String[] args)
            throws Throwable
    {
        Logging.initialize();

        File cwd = new File(System.getProperty("user.dir"));

        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        List<String> poms = ImmutableList.of(
                "presto-main/pom.xml",
                "presto-cassandra/pom.xml",
                "presto-example-http/pom.xml",
                "presto-hive-hadoop2/pom.xml",
                "presto-kafka/pom.xml",
                "presto-ml/pom.xml",
                "presto-mysql/pom.xml",
                "presto-postgresql/pom.xml",
                "presto-raptor/pom.xml",
                "presto-tpch/pom.xml",
                "presto-wrmsr-extensions/pom.xml"
        );

        for (String pom : poms) {
            log.info(pom);

            List<Artifact> artifacts = resolver.resolvePom(new File(cwd, pom));

            List<File> files = Lists.newArrayList();
            for (Artifact a : artifacts) {
                files.add(a.getFile());
            }

            for (File file : files) {
                if (!file.exists()) {
                    // throw new IllegalStateException(file.getAbsolutePath());
                    log.warn(String.format("File not found: %s", file));
                }
                URL url = new URL("file:" + file.getAbsolutePath() + (file.isDirectory() ? "/" : ""));
                log.info(url.toString());
            }
        }
    }
}

