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

package org.apache.hadoop.streaming;

import org.apache.hadoop.mapred.JobConf;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class PipeCombiner
        extends PipeReducer
{
    String getPipeCommand(JobConf job)
    {
        String str = job.get("stream.combine.streamprocessor");
        try {
            if (str != null) {
                return URLDecoder.decode(str, "UTF-8");
            }
        }
        catch (UnsupportedEncodingException e) {
            System.err.println("stream.combine.streamprocessor" +
                    " in jobconf not found");
        }
        return null;
    }

    boolean getDoPipe()
    {
        return (getPipeCommand(job_) != null);
    }
}
