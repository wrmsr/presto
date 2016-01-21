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
package com.wrmsr.presto.serialization.boxing;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Optional;
import java.util.Set;

public class BoxingManager
        implements BoxerProvider
{
    private volatile Set<BoxerProvider> boxerProviders;

    @Inject
    public BoxingManager(Set<BoxerProvider> boxerProviders)
    {
        this.boxerProviders = ImmutableSet.copyOf(boxerProviders);
    }

    public synchronized void addBoxerProvider(BoxerProvider boxerProvider)
    {
        boxerProviders = ImmutableSet.<BoxerProvider>builder()
                .addAll(boxerProviders)
                .add(boxerProvider)
                .build();
    }

    @Override
    public Optional<Boxer> getBoxer(Type type, Context context)
    {
        return null;
    }
}
