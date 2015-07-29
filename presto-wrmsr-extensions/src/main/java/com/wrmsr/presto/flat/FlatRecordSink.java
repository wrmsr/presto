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

import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.*;
import java.util.Collection;
import java.util.List;

public class FlatRecordSink
    implements RecordSink
{
    private final FileOutputStream f;
    private final BufferedWriter bw;

    public FlatRecordSink()
    {
        try {
            f = new FileOutputStream(new File(System.getProperty("user.home") + "/presto/flat/bye.txt"));
            bw = new BufferedWriter(new OutputStreamWriter(f));

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
    public void beginRecord(long sampleWeight)
    {

    }

    @Override
    public void finishRecord()
    {

    }

    @Override
    public void appendNull()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendBoolean(boolean value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendLong(long value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendDouble(double value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendString(byte[] value)
    {
        try {
            bw.write(new String(value));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void appendObject(Object value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Slice> commit()
    {
        close();
        // the committer can list the directory
        return ImmutableList.of();
    }

    @Override
    public void rollback()
    {
        close();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return ImmutableList.of(FlatMetadata.COLUMN_TYPE);
    }

    private void close()
    {
        try {
            bw.close();
            f.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
