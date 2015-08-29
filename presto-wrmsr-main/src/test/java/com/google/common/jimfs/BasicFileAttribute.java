/*
 * Copyright 2013 Google Inc.
 *
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

package com.google.common.jimfs;

import java.nio.file.attribute.FileAttribute;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Colin Decker
 */
public class BasicFileAttribute<T>
        implements FileAttribute<T>
{

    private final String name;
    private final T value;

    public BasicFileAttribute(String name, T value)
    {
        this.name = checkNotNull(name);
        this.value = checkNotNull(value);
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public T value()
    {
        return value;
    }
}
