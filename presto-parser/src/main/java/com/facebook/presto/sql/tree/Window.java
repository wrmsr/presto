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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Window
        extends Node
{
    private final WindowSpecification specification;

    public Window(WindowSpecification specification)
    {
        this(Optional.empty(), specification);
    }

    private Window(NodeLocation location, WindowSpecification specification)
    {
        this(Optional.of(location), specification);
    }

    private Window(Optional<NodeLocation> location, WindowSpecification specification)
    {
        super(location);
        this.specification = requireNonNull(specification, "specification is null");
    }

    public WindowSpecification getSpecification()
    {
        return specification;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindow(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Window o = (Window) obj;
        return Objects.equals(specification, o.specification);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .toString();
    }
}
