/**
 * Copyright 2013 Cloudera Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wrmsr.presto.util.kite;

import java.io.IOException;

/**
 * <p>
 * Exception thrown for dataset IO-related failures.
 * </p>
 *
 * @since 0.9.0
 */
public class DatasetIOException
        extends DatasetException
{
    private final IOException ioException;

    public DatasetIOException(String message, IOException root)
    {
        super(message, root);
        this.ioException = root;
    }

    public IOException getIOException()
    {
        return ioException;
    }
}
