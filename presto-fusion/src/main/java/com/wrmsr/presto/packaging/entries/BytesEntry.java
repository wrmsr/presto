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
package com.wrmsr.presto.packaging.entries;

import java.util.Arrays;

public class BytesEntry
        extends Entry
{
    public final byte[] bytes;

    public BytesEntry(String jarPath, byte[] bytes, long time)
    {
        super(jarPath, time);
        this.bytes = bytes;
    }

    public byte[] getBytes()
    {
        return bytes;
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
        if (!super.equals(o)) {
            return false;
        }

        BytesEntry that = (BytesEntry) o;

        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (bytes != null ? Arrays.hashCode(bytes) : 0);
        return result;
    }
}
