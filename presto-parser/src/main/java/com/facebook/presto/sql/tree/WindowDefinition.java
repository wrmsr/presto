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

public class WindowDefinition
        extends Node
{
    private final String alias;
    private final WindowSpecification specification;

    public WindowDefinition(String alias, WindowSpecification specification)
    {
        this(Optional.empty(), alias, specification);
    }

    public WindowDefinition(NodeLocation location, String alias, WindowSpecification specification)
    {
        this(Optional.of(location), alias, specification);
    }

    private WindowDefinition(Optional<NodeLocation> location, String alias, WindowSpecification specification)
    {
        super(location);
        this.alias = requireNonNull(alias, "alias is null");
        this.specification = requireNonNull(specification, "specification is null");
    }

    public String getAlias()
    {
        return alias;
    }

    public WindowSpecification getSpecification()
    {
        return specification;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowDefinition(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowDefinition that = (WindowDefinition) o;
        return Objects.equals(alias, that.alias) &&
                Objects.equals(specification, that.specification);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(alias, specification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("alias", alias)
                .add("specification", specification)
                .toString();
    }
}
