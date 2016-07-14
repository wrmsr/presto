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

public class WindowAlias
        extends Window
{
    private final String alias;

    public WindowAlias(String alias)
    {
        this(Optional.empty(), alias);
    }

    public WindowAlias(NodeLocation location, String alias)
    {
        this(Optional.of(location), alias);
    }

    private WindowAlias(Optional<NodeLocation> location, String alias)
    {
        super(location);
        this.alias = requireNonNull(alias, "alias is null");
    }

    public String getAlias()
    {
        return alias;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowAlias(this, context);
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
        WindowAlias that = (WindowAlias) o;
        return Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(alias);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("alias", alias)
                .toString();
    }
}
