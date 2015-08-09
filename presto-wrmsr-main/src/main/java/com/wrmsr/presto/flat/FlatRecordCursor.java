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
package com.wrmsr.presto.flat;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FlatRecordCursor
    implements RecordCursor
{
    private final FileInputStream f;
    private final BufferedReader br;
    @Nullable
    private String next = null;

    public FlatRecordCursor()
    {
        try {
            f = new FileInputStream(new File(System.getProperty("user.home") + "/presto/flat/hi.txt"));
            br = new BufferedReader(new InputStreamReader(f));

            /*
            String line;
            while ((line = br.readLine()) != null) {
                System.out.printf("%s%n", line);
            }
            */
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field == 0);
        return FlatMetadata.COLUMN_TYPE;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (next != null) {
            return true;
        }
        try {
            next = br.readLine();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return next != null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        throw new NotImplementedException();
    }

    @Override
    public long getLong(int field)
    {
        throw new NotImplementedException();
    }

    @Override
    public double getDouble(int field)
    {
        throw new NotImplementedException();
    }

    @Override
    public Slice getSlice(int field)
    {
        if (next == null) {
            try {
                next = br.readLine();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            checkNotNull(next);
        }
        String ret = next;
        next = null;
        return Slices.wrappedBuffer(ret.getBytes());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        try {
            br.close();
            f.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
