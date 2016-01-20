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

package com.wrmsr.presto.ffi.hadont.streaming;

//import com.wrmsr.presto.ffi.hadont.mapred.MapRunner;
//import com.wrmsr.presto.ffi.hadont.mapred.OutputCollector;
//import com.wrmsr.presto.ffi.hadont.mapred.RecordReader;
//import com.wrmsr.presto.ffi.hadont.mapred.Reporter;
//
//import java.io.IOException;
//
//public class PipeMapRunner<K1, V1, K2, V2>
//        extends MapRunner<K1, V1, K2, V2>
//{
//    public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
//            Reporter reporter)
//            throws IOException
//    {
//        PipeMapper pipeMapper = (PipeMapper) getMapper();
//        pipeMapper.startOutputThreads(output, reporter);
//        super.run(input, output, reporter);
//    }
//}
