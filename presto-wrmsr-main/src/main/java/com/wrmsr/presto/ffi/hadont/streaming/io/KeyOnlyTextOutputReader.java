/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wrmsr.presto.ffi.hadont.streaming.io;

import com.wrmsr.presto.ffi.hadont.io.NullWritable;
import com.wrmsr.presto.ffi.hadont.io.Text;
import com.wrmsr.presto.ffi.hadont.streaming.PipeMapRed;
import com.wrmsr.presto.ffi.hadont.util.LineReader;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

/**
 * OutputReader that reads the client's output as text, interpreting each line
 * as a key and outputting NullWritables for values.
 */
public class KeyOnlyTextOutputReader
        extends OutputReader<Text, NullWritable>
{
    private LineReader lineReader;
    private byte[] bytes;
    private DataInput clientIn;
    private Text key;
    private Text line;

    public static final int BUFFER_SIZE = 4096;

    @Override
    public void initialize(PipeMapRed pipeMapRed)
            throws IOException
    {
        super.initialize(pipeMapRed);
        clientIn = pipeMapRed.getClientInput();
        lineReader = new LineReader((InputStream) clientIn, BUFFER_SIZE);
        key = new Text();
        line = new Text();
    }

    @Override
    public boolean readKeyValue()
            throws IOException
    {
        if (lineReader.readLine(line) <= 0) {
            return false;
        }
        bytes = line.getBytes();
        key.set(bytes, 0, line.getLength());

        line.clear();
        return true;
    }

    @Override
    public Text getCurrentKey()
            throws IOException
    {
        return key;
    }

    @Override
    public NullWritable getCurrentValue()
            throws IOException
    {
        return NullWritable.get();
    }

    @Override
    public String getLastOutput()
    {
        if (bytes != null) {
            try {
                return new String(bytes, "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                return "<undecodable>";
            }
        }
        else {
            return null;
        }
    }
}
