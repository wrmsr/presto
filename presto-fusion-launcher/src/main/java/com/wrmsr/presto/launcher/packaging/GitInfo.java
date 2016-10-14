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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkState;
import static com.wrmsr.presto.util.Strings.stripString;
import static java.util.Objects.requireNonNull;

public final class GitInfo
{
    private final Path topLevel;
    private final String revision;
    private final String shortRevision;
    private final String tags;
    private final boolean isModified;
    private final boolean hasUntracked;

    public GitInfo(
            Path topLevel,
            String revision,
            String shortRevision,
            String tags,
            boolean isModified,
            boolean hasUntracked)
    {
        this.topLevel = requireNonNull(topLevel);
        this.revision = requireNonNull(revision);
        this.shortRevision = requireNonNull(shortRevision);
        this.tags = requireNonNull(tags);
        this.isModified = isModified;
        this.hasUntracked = hasUntracked;
    }

    public Path getTopLevel()
    {
        return topLevel;
    }

    public String getRevision()
    {
        return revision;
    }

    public String getShortRevision()
    {
        return shortRevision;
    }

    public String getTags()
    {
        return tags;
    }

    public boolean isModified()
    {
        return isModified;
    }

    public boolean hasUntracked()
    {
        return hasUntracked;
    }

    public static GitInfo get()
            throws IOException
    {
        Path topLevel = Paths.get(stripString(requireNonNull(Packager.shellExec("git", "rev-parse", "--show-toplevel"))));
        checkState(topLevel.toFile().exists() && topLevel.toFile().isDirectory());
        String revision = stripString(requireNonNull(Packager.shellExec("git", "rev-parse", "--verify", "HEAD"))); // FIXME append -DIRTY if dirty
        String shortRevision = stripString(requireNonNull(Packager.shellExec("git", "rev-parse", "--short", "HEAD"))); // FIXME append -DIRTY if dirty
        String tags = stripString(requireNonNull(Packager.shellExec("git", "describe", "--tags")));
        String shortStat = Packager.shellExec("git", "diff", "--shortstat");
        boolean isModified = shortStat != null && !stripString(shortStat).isEmpty();
        String porcelain = Packager.shellExec("git", "status", "--porcelain");
        boolean hasUntracked = porcelain != null && Arrays.stream(porcelain.split("\n")).anyMatch(s -> s.startsWith("??"));

        return new GitInfo(
                topLevel,
                revision,
                shortRevision,
                tags,
                isModified,
                hasUntracked);
    }
}
