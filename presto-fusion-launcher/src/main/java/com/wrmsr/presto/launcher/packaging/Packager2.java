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
import com.sun.org.apache.xerces.internal.dom.DeferredElementImpl;
import io.airlift.resolver.ArtifactResolver;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.annotation.concurrent.Immutable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class Packager2
{
    private final ArtifactResolver artifactResolver;

    @Immutable
    private static final class Module
    {
        private final File pomFile;
        private final Document pom;
        private final String name;
        private final Set<String> classPath;

        public Module(File pomFile, Document pom, String name, Set<String> classPath)
        {
            this.pomFile = pomFile;
            this.pom = pom;
            this.name = name;
            this.classPath = classPath;
        }
    }

    private final Map<String, Module> modules = new HashMap<>();
    private Module mainModule;

    public Packager2(ArtifactResolver artifactResolver)
    {
        this.artifactResolver = requireNonNull(artifactResolver);
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

        Document pom = readXml(pomFile);

        // checkState(!modules.containsKey(pom));

        System.out.println(pom);

        Node groupId = pom.getDocumentElement().getChildNodes().item(5);
        pom.getDocumentElement().getElementsByTagNameNS(groupId.getNamespaceURI(), groupId.getNodeName());
        throw new IllegalArgumentException();
    }

    public static void main(String[] args)
            throws Exception
    {
        ArtifactResolver resolver = new ArtifactResolver(
                ArtifactResolver.USER_LOCAL_REPO,
                ImmutableList.of(ArtifactResolver.MAVEN_CENTRAL_URI));

        Packager2 p = new Packager2(resolver);
        p.addMainModule(new File("/Users/spinlock/src/wrmsr/presto/presto-fusion-launcher/pom.xml"));
    }
}
