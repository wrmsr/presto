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

import com.google.common.collect.ImmutableMap;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactCoordinate;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactName;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.DefaultModelReader;
import org.apache.maven.model.io.ModelReader;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public final class Models
{
    private Models()
    {
    }

    public static Model readModel(File pomFile)
            throws IOException
    {
        checkArgument(pomFile.isFile());
        return new DefaultModelReader().read(pomFile, ImmutableMap.of(ModelReader.IS_STRICT, true));
    }

    public static Model readModelModule(Model model, String name)
            throws IOException
    {
        checkArgument(model.getModules().stream().anyMatch(name::equals));
        return readModel(new File(model.getProjectDirectory(), name + "/pom.xml"));
    }

    public static Map<String, Model> readModelModules(Model model)
            throws IOException
    {
        Map<String, Model> modules = new LinkedHashMap<>();
        for (String module : model.getModules()) {
            checkState(!modules.containsKey(module));
            modules.put(module, readModelModule(model, module));
        }
        return modules;
    }

    public static ArtifactName getModelArtifactName(Model model)
    {
        return new ArtifactName(
                model.getGroupId() != null ? model.getGroupId() : model.getParent() != null ? model.getParent().getGroupId() : null,
                model.getArtifactId());
    }

    public static ArtifactCoordinate getModelArtifactCoordinate(Model model)
    {
        return new ArtifactCoordinate(
                getModelArtifactName(model),
                model.getVersion());
    }
}
