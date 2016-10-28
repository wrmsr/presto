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
package com.wrmsr.presto.launcher.packaging.modules.transforms;

import com.wrmsr.presto.launcher.packaging.Models;
import com.wrmsr.presto.launcher.packaging.Packager;
import com.wrmsr.presto.launcher.packaging.artifacts.ArtifactName;
import com.wrmsr.presto.launcher.packaging.modules.PackagerModule;
import org.apache.maven.model.Model;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.collect.MoreCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@Immutable
public final class ProjectModulePackagerModuleTransform
        implements PackagerModuleTransform
{
    private final Model parentModel;

    private Map<ArtifactName, Model> modules;

    public ProjectModulePackagerModuleTransform(Model parentModel)
    {
        this.parentModel = requireNonNull(parentModel);
    }

    @Override
    public List<PackagerModule> apply(Packager packager, List<PackagerModule> packagerModules)
            throws IOException
    {
        if (modules == null) {
            modules = Models.readModelModules(parentModel).values().stream()
                    .collect(toImmutableMap(Models::getModelArtifactName, identity()));
        }
        List<PackagerModule> newPackagerModules = new ArrayList<>();
        for (PackagerModule packagerModule : packagerModules) {
            Model moduleModel = modules.get(packagerModule.getArtifactCoordinate().getName());
            if (moduleModel != null) {
                String jarFileName = moduleModel.getArtifactId() + "-" + Models.getModelOrParentVersion(moduleModel) + ".jar";
                File jarFile = new File(moduleModel.getPomFile().getParentFile(), "target/" + jarFileName);
                checkState(jarFile.isFile());
                newPackagerModules.add(new PackagerModule(
                        packagerModule.getArtifactCoordinate(),
                        Optional.of(jarFile),
                        packagerModule.getClassPath()));
            }
            else {
                newPackagerModules.add(packagerModule);
            }
        }
        return newPackagerModules;
    }
}
