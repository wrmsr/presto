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
package com.wrmsr.presto.functions;

import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import com.wrmsr.presto.util.Exceptions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nonnull;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class GrokFunctions
{
    public static final String PATTERNS_RESOURCE = "grok-patterns/grok-patterns";

    private static final ThreadLocalCache<String, Grok> GROK_CACHE = new ThreadLocalCache<String, Grok>(100)
    {
        @Nonnull
        @Override
        protected Grok load(String key)
        {
            try {
                Grok grok = new Grok();
                try (InputStream is = GrokFunctions.class.getClassLoader().getResourceAsStream(PATTERNS_RESOURCE);
                        InputStreamReader isr = new InputStreamReader(is);
                        BufferedReader br = new BufferedReader(isr)) {
                    grok.addPatternFromReader(br);
                }

                grok.compile(key);
                return grok;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    };

    @ScalarFunction("grok")
    @SqlType("map<varchar,varchar>")
    public static Block grok(@SqlType(StandardTypes.VARCHAR) Slice pat, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            Grok grok = GROK_CACHE.get(pat.toStringUtf8());
            Match gm = grok.match(value.toStringUtf8());
            gm.captures();

            Map<String, Object> map = gm.toMap();
            BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR), new BlockBuilderStatus(), map.size() * 2);

            for (Map.Entry<String, Object> e : map.entrySet()) {
                VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(e.getKey()));
                Object valueObj = e.getValue();
                if (valueObj == null) {
                    blockBuilder.appendNull();
                }
                else {
                    String valueStr = valueObj instanceof String ? (String) valueObj : valueObj.toString();
                    VarcharType.VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(valueStr));
                }
            }

            return blockBuilder.build();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
