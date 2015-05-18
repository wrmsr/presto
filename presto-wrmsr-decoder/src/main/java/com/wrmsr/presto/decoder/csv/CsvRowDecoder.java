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
package com.wrmsr.presto.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.wrmsr.presto.decoder.DecoderColumnHandle;
import com.wrmsr.presto.decoder.FieldDecoder;
import com.wrmsr.presto.decoder.FieldValueProvider;
import com.wrmsr.presto.decoder.RowDecoder;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Decode a  message as CSV. This is an extremely primitive CSV decoder using {@link au.com.bytecode.opencsv.CSVParser]}.
 */
public class CsvRowDecoder
        implements RowDecoder
{
    public static final String NAME = "csv";

    private final CSVParser parser = new CSVParser();

    @Inject
    CsvRowDecoder()
    {
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<FieldValueProvider> fieldValueProviders, List<DecoderColumnHandle> columnHandles, Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        String[] fields;
        try {
            // TODO - There is no reason why the row can't have a formatHint and it could be used
            // to set the charset here.
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            return true;
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            String mapping = columnHandle.getMapping();
            checkState(mapping != null, "No mapping for column handle %s!", columnHandle);
            int columnIndex = Integer.parseInt(mapping);

            if (columnIndex >= fields.length) {
                continue;
            }

            @SuppressWarnings("unchecked")
            FieldDecoder<String> decoder = (FieldDecoder<String>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                fieldValueProviders.add(decoder.decode(fields[columnIndex], columnHandle));
            }
        }
        return false;
    }
}
