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
package com.wrmsr.presto.metaconnectors.codec.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.wrmsr.presto.metaconnectors.codec.CodecColumnHandle;
import com.wrmsr.presto.metaconnectors.codec.FieldCodec;
import com.wrmsr.presto.metaconnectors.codec.FieldValueProvider;
import com.wrmsr.presto.metaconnectors.codec.RowCodec;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * JSON specific  row decoder.
 */
public class JsonRowCodec
        implements RowCodec
{
    public static final String NAME = "json";

    private final ObjectMapper objectMapper;

    @Inject
    JsonRowCodec(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<FieldValueProvider> fieldValueProviders, List<CodecColumnHandle> columnHandles, Map<CodecColumnHandle, FieldCodec<?>> fieldDecoders)
    {
        JsonNode tree;

        try {
            tree = objectMapper.readTree(data);
        }
        catch (Exception e) {
            return true;
        }

        for (CodecColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            FieldCodec<JsonNode> decoder = (FieldCodec<JsonNode>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                JsonNode node = locateNode(tree, columnHandle);
                fieldValueProviders.add(decoder.decode(node, columnHandle));
            }
        }

        return false;
    }

    private static JsonNode locateNode(JsonNode tree, CodecColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}
