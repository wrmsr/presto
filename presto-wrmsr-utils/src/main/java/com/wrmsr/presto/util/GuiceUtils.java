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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateBinder;
import com.google.inject.Scope;
import com.google.inject.internal.Errors;
import com.google.inject.spi.DefaultBindingScopingVisitor;
import com.google.inject.spi.DefaultElementVisitor;
import com.google.inject.spi.Element;
import com.google.inject.spi.Elements;
import com.google.inject.spi.PrivateElements;
import com.google.inject.spi.ScopeBinding;
import com.google.inject.util.Modules;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class GuiceUtils
{
    private GuiceUtils()
    {
    }

    /**
     * See the EDSL example at {@link Modules#override(Module[]) override()}.
     */
    public interface OverriddenModuleBuilder
    {
        /**
         * See the EDSL example at {@link Modules#override(Module[]) override()}.
         */
        Module with(Module... overrides);

        /**
         * See the EDSL example at {@link Modules#override(Module[]) override()}.
         */
        Module with(Iterable<? extends Module> overrides);
    }

    public static class CombineConfigurationAwareModule
            extends AbstractConfigurationAwareModule
    {
        private final Set<Module> modulesSet;
        private ConfigurationFactory configurationFactory; // wow fuck you.

        public CombineConfigurationAwareModule(Iterable<? extends Module> modules)
        {
            this.modulesSet = ImmutableSet.copyOf(modules);
        }

        @Override
        public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory)
        {
            super.setConfigurationFactory(configurationFactory);
            this.configurationFactory = configurationFactory;
        }

        @Override
        protected void setup(Binder binder)
        {
            binder = binder.skipSources(getClass());
            for (Module module : modulesSet) {
                if (module instanceof ConfigurationAwareModule) {
                    ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
                }
                binder.install(module);
            }
        }
    }

    public static final class RealOverriddenModuleBuilder
            implements OverriddenModuleBuilder
    {
        private final ImmutableSet<Module> baseModules;

        private RealOverriddenModuleBuilder(Iterable<? extends Module> baseModules)
        {
            this.baseModules = ImmutableSet.copyOf(baseModules);
        }

        public Module with(Module... overrides)
        {
            return with(Arrays.asList(overrides));
        }

        public Module with(Iterable<? extends Module> overrides)
        {
            return new OverrideConfigurationAwareModule(overrides, baseModules);
        }
    }

    public static class OverrideConfigurationAwareModule
            extends AbstractModule
            implements ConfigurationAwareModule
    {
        private final ImmutableSet<Module> overrides;
        private final ImmutableSet<Module> baseModules;
        private Binder binder;

        public OverrideConfigurationAwareModule(Iterable<? extends Module> overrides, ImmutableSet<Module> baseModules)
        {
            this.overrides = ImmutableSet.copyOf(overrides);
            this.baseModules = baseModules;
        }

        private ConfigurationFactory configurationFactory; // wow fuck you.

        @Override
        public synchronized void setConfigurationFactory(ConfigurationFactory configurationFactory)
        {
            this.configurationFactory = checkNotNull(configurationFactory, "configurationFactory is null");
            for (Module module : baseModules) {
                if (module instanceof ConfigurationAwareModule) {
                    ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
                }
            }
            for (Module module : overrides) {
                if (module instanceof ConfigurationAwareModule) {
                    ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
                }
            }
        }

        @Override
        public void configure()
        {
            Binder baseBinder = binder();
            List<Element> baseElements = Elements.getElements(currentStage(), baseModules);

            // If the sole element was a PrivateElements, we want to override
            // the private elements within that -- so refocus our elements
            // and binder.
            if (baseElements.size() == 1) {
                Element element = Iterables.getOnlyElement(baseElements);
                if (element instanceof PrivateElements) {
                    PrivateElements privateElements = (PrivateElements) element;
                    PrivateBinder privateBinder = baseBinder.newPrivateBinder().withSource(privateElements.getSource());
                    for (Key exposed : privateElements.getExposedKeys()) {
                        privateBinder.withSource(privateElements.getExposedSource(exposed)).expose(exposed);
                    }
                    baseBinder = privateBinder;
                    baseElements = privateElements.getElements();
                }
            }

            final Binder binder = baseBinder.skipSources(this.getClass());
            final LinkedHashSet<Element> elements = new LinkedHashSet<Element>(baseElements);
            final List<Element> overrideElements = Elements.getElements(currentStage(), overrides);

            final Set<Key<?>> overriddenKeys = Sets.newHashSet();
            final Map<Class<? extends Annotation>, ScopeBinding> overridesScopeAnnotations =
                    new HashMap<>();

            // execute the overrides module, keeping track of which keys and scopes are bound
            new ModuleWriter(binder)
            {
                @Override
                public <T> Void visit(Binding<T> binding)
                {
                    overriddenKeys.add(binding.getKey());
                    return super.visit(binding);
                }

                @Override
                public Void visit(ScopeBinding scopeBinding)
                {
                    overridesScopeAnnotations.put(scopeBinding.getAnnotationType(), scopeBinding);
                    return super.visit(scopeBinding);
                }

                @Override
                public Void visit(PrivateElements privateElements)
                {
                    overriddenKeys.addAll(privateElements.getExposedKeys());
                    return super.visit(privateElements);
                }
            }.writeAll(overrideElements);

            // execute the original module, skipping all scopes and overridden keys. We only skip each
            // overridden binding once so things still blow up if the module binds the same thing
            // multiple times.
            final Map<Scope, List<Object>> scopeInstancesInUse = Maps.newHashMap();
            final List<ScopeBinding> scopeBindings = Lists.newArrayList();
            new ModuleWriter(binder)
            {
                @Override
                public <T> Void visit(Binding<T> binding)
                {
                    if (!overriddenKeys.remove(binding.getKey())) {
                        super.visit(binding);

                        // Record when a scope instance is used in a binding
                        Scope scope = getScopeInstanceOrNull(binding);
                        if (scope != null) {
                            List<Object> existing = scopeInstancesInUse.get(scope);
                            if (existing == null) {
                                existing = Lists.newArrayList();
                                scopeInstancesInUse.put(scope, existing);
                            }
                            existing.add(binding.getSource());
                        }
                    }

                    return null;
                }

                void rewrite(Binder binder, PrivateElements privateElements, Set<Key<?>> keysToSkip)
                {
                    PrivateBinder privateBinder = binder.withSource(privateElements.getSource())
                            .newPrivateBinder();

                    Set<Key<?>> skippedExposes = Sets.newHashSet();

                    for (Key<?> key : privateElements.getExposedKeys()) {
                        if (keysToSkip.remove(key)) {
                            skippedExposes.add(key);
                        }
                        else {
                            privateBinder.withSource(privateElements.getExposedSource(key)).expose(key);
                        }
                    }

                    for (Element element : privateElements.getElements()) {
                        if (element instanceof Binding
                                && skippedExposes.remove(((Binding) element).getKey())) {
                            continue;
                        }
                        if (element instanceof PrivateElements) {
                            rewrite(privateBinder, (PrivateElements) element, skippedExposes);
                            continue;
                        }
                        element.applyTo(privateBinder);
                    }
                }

                @Override
                public Void visit(PrivateElements privateElements)
                {
                    rewrite(binder, privateElements, overriddenKeys);
                    return null;
                }

                @Override
                public Void visit(ScopeBinding scopeBinding)
                {
                    scopeBindings.add(scopeBinding);
                    return null;
                }
            }.writeAll(elements);

            // execute the scope bindings, skipping scopes that have been overridden. Any scope that
            // is overridden and in active use will prompt an error
            new ModuleWriter(binder)
            {
                @Override
                public Void visit(ScopeBinding scopeBinding)
                {
                    ScopeBinding overideBinding =
                            overridesScopeAnnotations.remove(scopeBinding.getAnnotationType());
                    if (overideBinding == null) {
                        super.visit(scopeBinding);
                    }
                    else {
                        List<Object> usedSources = scopeInstancesInUse.get(scopeBinding.getScope());
                        if (usedSources != null) {
                            StringBuilder sb = new StringBuilder(
                                    "The scope for @%s is bound directly and cannot be overridden.");
                            sb.append("%n     original binding at " + Errors.convert(scopeBinding.getSource()));
                            for (Object usedSource : usedSources) {
                                sb.append("%n     bound directly at " + Errors.convert(usedSource) + "");
                            }
                            binder.withSource(overideBinding.getSource())
                                    .addError(sb.toString(), scopeBinding.getAnnotationType().getSimpleName());
                        }
                    }
                    return null;
                }
            }.writeAll(scopeBindings);
        }

        private Scope getScopeInstanceOrNull(Binding<?> binding)
        {
            return binding.acceptScopingVisitor(new DefaultBindingScopingVisitor<Scope>()
            {
                @Override
                public Scope visitScope(Scope scope)
                {
                    return scope;
                }
            });
        }
    }

    public static class ModuleWriter
            extends DefaultElementVisitor<Void>
    {
        protected final Binder binder;

        public ModuleWriter(Binder binder)
        {
            this.binder = binder.skipSources(this.getClass());
        }

        @Override
        protected Void visitOther(Element element)
        {
            element.applyTo(binder);
            return null;
        }

        void writeAll(Iterable<? extends Element> elements)
        {
            for (Element element : elements) {
                element.acceptVisitor(this);
            }
        }
    }
}